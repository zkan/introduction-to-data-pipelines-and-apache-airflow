select
    temp

from {{ ref('stg_weathers') }}
where temp > 60