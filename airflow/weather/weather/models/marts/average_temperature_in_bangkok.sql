select
    avg(temp) as avg_temp

from {{ ref('stg_weathers') }}
