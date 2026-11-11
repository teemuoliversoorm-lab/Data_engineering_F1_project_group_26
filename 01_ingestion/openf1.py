"""Thin OpenF1 API client, plus the mapping from OpenF1's raw field names to
the warehouse field names (DriverId, LapTime, ...) that the dbt silver models
expect. See https://openf1.org/docs/ for the source API.
"""

import re
import time
import unicodedata
from bisect import bisect_right
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import requests

from ingestion.quality import DataQualityError

BASE_URL = "https://api.openf1.org/v1"
REQUEST_INTERVAL = 2.2  # seconds between requests -- be polite to a free public API

# Non-numeric classification codes OpenF1 can return in the "position" field.
STATUS_CODES = {
    "RT": "Retired", "DNF": "Did not finish", "DNS": "Did not start",
    "DSQ": "Disqualified", "DQ": "Disqualified", "NC": "Not classified", "WD": "Withdrawn",
}


class SourceUnavailableError(RuntimeError):
    """OpenF1 could not answer a request; the caller should fail the task
    rather than publish a guess."""


def parse_timestamp(value):
    if not isinstance(value, str):
        raise DataQualityError("OpenF1 timestamp is missing")
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def to_duration(value):
    """OpenF1 gives lap/qualifying times as plain seconds; the warehouse
    stores them as ISO-8601 durations, e.g. 92.5 -> 'PT92.5S'."""
    if value is None:
        return None
    if not isinstance(value, (int, float)) or value < 0:
        raise DataQualityError(f"OpenF1 invalid duration: {value!r}")
    return f"PT{value}S"


def identity(value, label):
    """Turn a free-text name into a stable, URL/DB-safe id, e.g.
    'Max Verstappen' -> 'max_verstappen'. OpenF1 has no immutable driver or
    team id, so a normalized name is the best available natural key."""
    if not value:
        raise DataQualityError(f"OpenF1 missing {label}")
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "_", value).strip("_")


class OpenF1Client:
    """Minimal GET client: one politeness delay between requests, a few
    retries on transient errors. Each instance only throttles its own
    requests -- it does NOT coordinate with other processes. OpenF1's actual
    rate limit is enforced across all callers, so the DAG serializes every
    extract task instance (`max_active_tis_per_dag=1` in 00_dags/f1_ingestion.py)
    to keep requests from different mapped tasks from overlapping."""

    def __init__(self, interval=REQUEST_INTERVAL, timeout=30, retries=4):
        self.interval = interval
        self.timeout = timeout
        self.retries = retries
        self._last_request = 0.0

    def get(self, endpoint, allow_empty=False, **filters):
        url = f"{BASE_URL}/{endpoint}"
        error = None
        for attempt in range(self.retries):
            self._wait()
            try:
                response = requests.get(url, params=filters, timeout=self.timeout)
                # OpenF1 answers a query with no matching rows as an HTTP 404,
                # not an empty JSON array -- treat it the same as [] here.
                if response.status_code == 404 and allow_empty:
                    return []
                if response.status_code == 429:
                    # Too Many Requests: back off for as long as OpenF1 asks
                    # (falling back to an escalating wait if it doesn't say),
                    # rather than the short generic retry backoff below.
                    wait = response.headers.get("Retry-After")
                    time.sleep(min(float(wait), 60) if wait else 2 ** (attempt + 2))
                    error = requests.exceptions.HTTPError(f"429 Too Many Requests: {url}")
                    continue
                response.raise_for_status()
                rows = response.json()
                break
            except requests.RequestException as exc:
                error = exc
                time.sleep(2 ** attempt)  # short backoff before retrying
        else:
            raise SourceUnavailableError(
                f"OpenF1 request failed after {self.retries} attempts: {url}"
            ) from error
        if not isinstance(rows, list):
            raise SourceUnavailableError(f"OpenF1 returned unexpected data: {url}")
        if not rows and not allow_empty:
            raise SourceUnavailableError(f"OpenF1 returned no historical data: {url}")
        return rows

    def _wait(self):
        delay = self.interval - (time.time() - self._last_request)
        if delay > 0:
            time.sleep(delay)
        self._last_request = time.time()


def schedule(client, season):
    """Map a season to its ordered, non-cancelled main Race sessions. The
    round number used everywhere in this pipeline is just the chronological
    position in this list -- OpenF1 has no separate "round" field."""
    sessions = client.get("sessions", year=season)
    races = sorted(
        (row for row in sessions if row["session_name"] == "Race" and not row.get("is_cancelled")),
        key=lambda row: row["date_start"],
    )
    if not races:
        raise DataQualityError(f"OpenF1 has no race calendar for {season}")
    return sessions, races


def _indexed_by_driver(rows):
    return {row["driver_number"]: row for row in rows}


def adapt(session_type, raw_laps, raw_results, drivers, grid, positions):
    """Reshape one session's raw OpenF1 responses into the laps/session_results
    records the bronze tables and dbt silver models expect
    (see 02_dbt/models/silver/)."""
    driver_map = _indexed_by_driver(drivers)
    classifications = _indexed_by_driver(raw_results)
    grid_map = _indexed_by_driver(grid)

    # A driver can appear in the lap data without an official classification
    # (e.g. retired before the finish). Keep them with null result metrics
    # instead of silently dropping their laps.
    participants = dict(classifications)
    for lap in raw_laps:
        participants.setdefault(lap["driver_number"], {"driver_number": lap["driver_number"]})

    results = [_adapt_result(number, row, driver_map[number], session_type, number in classifications,
                              grid_map.get(number, {}).get("position"))
               for number, row in sorted(participants.items())]

    # Group position samples per driver once, sorted by time, so each lap's
    # "position at lap end" lookup below is a cheap binary search instead of
    # a full re-scan per lap.
    samples_by_driver = defaultdict(list)
    for sample in positions:
        samples_by_driver[sample["driver_number"]].append((parse_timestamp(sample["date"]), sample["position"]))
    for samples in samples_by_driver.values():
        samples.sort(key=lambda item: item[0])

    laps = [_adapt_lap(row, samples_by_driver.get(row["driver_number"], []))
            for row in sorted(raw_laps, key=lambda r: (r["driver_number"], r["lap_number"]))]
    return {"laps": laps, "session_results": results}


def _adapt_result(number, row, driver, session_type, classification_available, grid_position):
    duration = row.get("duration") if session_type == "Q" else None
    q1, q2, q3 = (to_duration(value) for value in (duration or [None, None, None]))

    # OpenF1 flags dnf/dns/dsq as separate booleans and can still leave
    # `position` as a plain null for them (e.g. a post-race disqualification),
    # not a string code -- check the flags first so that case isn't lost.
    status = ("Disqualified" if row.get("dsq") else "Did not start" if row.get("dns")
              else "Did not finish" if row.get("dnf")
              else "Finished" if row.get("dnf") is False and row.get("dns") is False
              else None)
    position, status_code = row.get("position"), None
    if isinstance(position, str):
        status_code = position.strip().upper()
        status = STATUS_CODES.get(status_code, "Unknown")
        position = None

    return {
        "DriverNumber": str(number),
        "DriverId": "openf1:" + identity(driver.get("full_name"), "full_name"),
        "TeamId": "openf1:" + identity(driver.get("team_name"), "team_name"),
        "FullName": driver.get("full_name"),
        "Abbreviation": driver.get("name_acronym"),
        "TeamName": driver.get("team_name"),
        "CountryCode": driver.get("country_code"),
        "Position": position,
        "PositionStatusCode": status_code,
        "ClassificationAvailable": classification_available,
        "GridPosition": grid_position if session_type == "R" else None,
        "Q1": q1, "Q2": q2, "Q3": q3,
        "Points": row.get("points") if session_type == "R" else None,
        "Status": status,
    }


def _adapt_lap(row, samples):
    """One lap record. Lap-end position is approximate: OpenF1 has no
    per-lap position field, so we take the most recent position sample at or
    before this lap's end time (see docs/data_model.md)."""
    duration = row.get("lap_duration")
    lap_position, observed_at = None, None
    if duration is not None and row.get("date_start"):
        end = parse_timestamp(row["date_start"]) + timedelta(seconds=duration)
        times = [time for time, _ in samples]
        index = bisect_right(times, end) - 1
        if index >= 0:
            observed_at, lap_position = samples[index]
    return {
        "DriverNumber": str(row["driver_number"]),
        "LapNumber": row["lap_number"],
        "LapTime": to_duration(duration),
        "Position": lap_position,
        "TrackStatus": None,  # not available from OpenF1; see docs/data_model.md
        "PositionMethod": "nearest_sample_at_or_after_lap_end" if observed_at else None,
        "PositionObservedAt": observed_at.isoformat() if observed_at else None,
    }
