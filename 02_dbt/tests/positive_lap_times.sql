select * from {{ ref('factlap') }} where laptime <= 0
