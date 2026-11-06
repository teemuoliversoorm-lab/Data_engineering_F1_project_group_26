{{ config(indexes=[{'columns': ['driverkey'], 'unique': true}, {'columns': ['driverid'], 'unique': true}]) }}
select distinct on (driver_id)
    'driver:' || driver_id as driverkey,
    driver_id as driverid,
    driver_number::integer as drivernumber,
    full_name as fullname,
    abbreviation,
    country_code as countrycode
from {{ ref('session_results') }}
order by driver_id, season desc, round_number desc, session_type desc
