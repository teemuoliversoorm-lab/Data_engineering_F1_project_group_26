select 'lap' as fact, eventkey, driverkey
from {{ ref('factlap') }}
group by eventkey, driverkey, sessiontype, lapnumber having count(*) > 1
union all
select 'result', eventkey, driverkey
from {{ ref('factdrivereventresult') }}
group by eventkey, driverkey having count(*) > 1
