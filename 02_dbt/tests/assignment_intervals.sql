select a.driverteamassignmentkey
from {{ ref('dimdriverteamassignment') }} a
join {{ ref('dimdriverteamassignment') }} b
    on a.driverkey = b.driverkey
    and a.driverteamassignmentkey < b.driverteamassignmentkey
    and daterange(a.effectivedate, a.expirationdate, '[)')
        && daterange(b.effectivedate, b.expirationdate, '[)')
