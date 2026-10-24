"""Public historical OpenF1 client and explicit warehouse field adapter."""

import json
import math
import re
import threading
import time
import unicodedata
from bisect import bisect_right
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ingestion.contracts import DataQualityError, positive_integer

BASE_URL = "https://api.openf1.org/v1"
ADAPTER_VERSION = "openf1-v2"
_PROCESS_LOCK = threading.Lock()


class SourceUnavailableError(RuntimeError):
    """A source request failed; no replacement batch should be published."""


def timestamp(value):
    if not isinstance(value, str):
        raise DataQualityError("OpenF1 timestamp is missing")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise DataQualityError("OpenF1 timestamp must include a timezone")
    return result.astimezone(timezone.utc)


class OpenF1Client:
    def __init__(self, state_dir=None, interval=2.2, timeout=45, attempts=4,
                 opener=None, sleep=None, clock=None):
        self.interval = interval
        self.timeout = timeout
        self.attempts = attempts
        self.opener = opener or urlopen
        self.sleep = sleep or time.sleep
        self.clock = clock or time.time
        self.responses = {}
        self.last_request = 0.0
        self.state_path = None
        if state_dir is not None:
            directory = Path(state_dir)
            directory.mkdir(parents=True, exist_ok=True)
            self.state_path = directory / "openf1-rate-limit"

    def _wait(self):
        # Shared on the Docker cache volume: mapped tasks and CLI runs share a limit.
        with _PROCESS_LOCK:
            if self.state_path is None:
                delay = self.interval - (self.clock() - self.last_request)
                if delay > 0:
                    self.sleep(delay)
                self.last_request = self.clock()
                return
            with self.state_path.open("a+", encoding="ascii") as state:
                try:
                    import fcntl
                except ImportError:  # Windows local development; one process only.
                    fcntl = None
                if fcntl:
                    fcntl.flock(state, fcntl.LOCK_EX)
                try:
                    state.seek(0)
                    previous = float(state.read() or 0)
                    delay = self.interval - (self.clock() - previous)
                    if delay > 0:
                        self.sleep(delay)
                    state.seek(0)
                    state.truncate()
                    state.write(str(self.clock()))
                    state.flush()
                finally:
                    if fcntl:
                        fcntl.flock(state, fcntl.LOCK_UN)

    def get(self, endpoint, allow_empty=False, **filters):
        if not re.fullmatch(r"[a-z_]+", endpoint):
            raise ValueError("Invalid OpenF1 endpoint")
        query = endpoint + "?" + urlencode(sorted(filters.items()))
        url = BASE_URL + "/" + query
        for attempt in range(self.attempts):
            self._wait()
            try:
                request = Request(url, headers={"User-Agent": "F1-Group26-education/1.0"})
                with self.opener(request, timeout=self.timeout) as response:
                    rows = json.load(response)
                if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                    raise SourceUnavailableError(f"OpenF1 returned an invalid JSON array: {url}")
                if not rows and not allow_empty:
                    raise SourceUnavailableError(f"OpenF1 returned no historical data: {url}")
                # Stable ordering makes repeated equivalent responses idempotent.
                rows = sorted(rows, key=lambda row: json.dumps(row, sort_keys=True, allow_nan=False))
                self.responses[query] = {"url": url, "records": rows}
                return rows
            except HTTPError as exc:
                body = exc.read().decode("utf-8", errors="replace")
                if exc.code == 404 and allow_empty:
                    try:
                        missing = json.loads(body).get("detail") == "No results found."
                    except (ValueError, AttributeError):
                        missing = False
                    if missing:
                        self.responses[query] = {"url": url, "records": []}
                        return []
                if exc.code not in (429, 500, 502, 503, 504) or attempt + 1 == self.attempts:
                    raise SourceUnavailableError(f"OpenF1 HTTP {exc.code}: {url}; {body[:200]}") from exc
                try:
                    wait = float(exc.headers.get("Retry-After", 2 ** (attempt + 1)))
                except (TypeError, ValueError):
                    wait = 2 ** (attempt + 1)
                self.sleep(min(max(wait, 0), 60))
            except (URLError, TimeoutError, OSError) as exc:
                if attempt + 1 == self.attempts:
                    raise SourceUnavailableError(f"OpenF1 request failed: {url}: {exc}") from exc
                self.sleep(min(2 ** (attempt + 1), 60))
            except (ValueError, UnicodeError) as exc:
                raise SourceUnavailableError(f"OpenF1 returned invalid JSON: {url}") from exc
        raise SourceUnavailableError(f"OpenF1 request exhausted retries: {url}")


def schedule(client, season):
    sessions = client.get("sessions", year=season)
    races = sorted(
        (row for row in sessions if row.get("session_name") == "Race"
         and not row.get("is_cancelled", False)),
        key=lambda row: (timestamp(row["date_start"]), row["session_key"]),
    )
    if not races or len({row["meeting_key"] for row in races}) != len(races):
        raise DataQualityError("OpenF1 race calendar is empty or ambiguous")
    return sessions, races


def identity(value, label):
    if not isinstance(value, str) or not value.strip():
        raise DataQualityError(f"OpenF1 missing {label}")
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value).strip("_")
    if not value:
        raise DataQualityError(f"OpenF1 invalid {label}")
    return value


def indexed(rows, label):
    result = {}
    for row in rows:
        key = positive_integer(row.get("driver_number"), f"{label}.driver_number")
        if key in result:
            raise DataQualityError(f"OpenF1 duplicate driver in {label}: {key}")
        result[key] = row
    return result


def seconds(value):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise DataQualityError(f"OpenF1 invalid duration: {value!r}")
    return f"PT{value}S"


def adapt(session_type, raw_laps, raw_results, drivers, grid, positions):
    driver_map = indexed(drivers, "drivers")
    classifications = indexed(raw_results, "session_result")
    result_map = dict(classifications)
    # Real lap observations establish participation even when classification is absent.
    # Keep their driver metadata with null result metrics and an explicit availability flag.
    for lap in raw_laps:
        number = positive_integer(lap.get("driver_number"), "laps.driver_number")
        result_map.setdefault(number, {"driver_number": number})
    grid_map = indexed(grid, "starting_grid")
    results = []
    for number, row in sorted(result_map.items()):
        driver = driver_map.get(number)
        if driver is None:
            raise DataQualityError(f"OpenF1 missing driver metadata: {number}")
        duration = row.get("duration")
        if session_type == "Q":
            if duration is None:
                duration = [None, None, None]
            if not isinstance(duration, list) or len(duration) != 3:
                raise DataQualityError("OpenF1 qualifying duration must contain Q1/Q2/Q3")
            stages = [seconds(value) for value in duration]
        else:
            stages = [None, None, None]
        # Classification comes from session_result, never the last position sample.
        status = ("Disqualified" if row.get("dsq") else "Did not start" if row.get("dns")
                  else "Did not finish" if row.get("dnf")
                  else "Finished" if row.get("dnf") is False and row.get("dns") is False
                  else None)
        position = row.get("position")
        position_code = None
        status_codes = {"RT": "Retired", "DNF": "Did not finish", "DNS": "Did not start",
                        "DSQ": "Disqualified", "DQ": "Disqualified",
                        "NC": "Not classified", "WD": "Withdrawn"}
        if isinstance(position, str):
            token = position.strip().upper()
            if token in status_codes:
                position_code = token
                status = status_codes[token]
                position = None
            elif token.isdigit():
                position = int(token)
            else:
                raise DataQualityError(f"OpenF1 unknown classification position: {position!r}")
        results.append({
            "DriverNumber": str(number),
            "DriverId": "openf1:" + identity(driver.get("full_name"), "full_name"),
            "TeamId": "openf1:" + identity(driver.get("team_name"), "team_name"),
            "FullName": driver["full_name"], "Abbreviation": driver.get("name_acronym"),
            "TeamName": driver["team_name"], "CountryCode": driver.get("country_code"),
            "Position": position,
            "PositionStatusCode": position_code,
            "ClassificationAvailable": number in classifications,
            "GridPosition": grid_map.get(number, {}).get("position") if session_type == "R" else None,
            "Q1": stages[0], "Q2": stages[1], "Q3": stages[2],
            "Points": row.get("points") if session_type == "R" else None, "Status": status,
            "_openf1": classifications.get(number),
            "_openf1_driver": driver,
        })
    changes = defaultdict(list)
    for row in positions:
        number = positive_integer(row.get("driver_number"), "position.driver_number")
        changes[number].append((timestamp(row["date"]), row.get("position")))
    for values in changes.values():
        values.sort(key=lambda item: item[0])
    times = {number: [item[0] for item in values] for number, values in changes.items()}
    laps = []
    for row in sorted(raw_laps, key=lambda item: (item["driver_number"], item["lap_number"])):
        number = positive_integer(row.get("driver_number"), "laps.driver_number")
        driver = driver_map.get(number)
        if driver is None or number not in result_map:
            raise DataQualityError(f"OpenF1 lap driver absent from metadata/results: {number}")
        duration = row.get("lap_duration")
        lap_time = seconds(duration)
        lap_position = None
        observation = None
        if duration is not None and row.get("date_start"):
            end = timestamp(row["date_start"]) + timedelta(seconds=duration)
            index = bisect_right(times.get(number, []), end) - 1
            if index >= 0:
                observation = changes[number][index]
                lap_position = observation[1]
        laps.append({
            "DriverNumber": str(number), "LapNumber": row["lap_number"], "LapTime": lap_time,
            "Position": lap_position, "TrackStatus": None, "Time": None,
            "Driver": driver.get("name_acronym"), "Team": driver["team_name"],
            "PositionMethod": "last_observation_at_approximate_lap_end" if observation else None,
            "PositionObservedAt": observation[0].isoformat() if observation else None,
            "_openf1": row,
        })
    return {"laps": laps, "session_results": results}
