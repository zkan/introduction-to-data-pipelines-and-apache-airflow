select
    TIMEZONE('Asia/Bangkok', TO_TIMESTAMP(dt)) as timestamp_gmt7
    , temp

from {{ source('open_weathers', 'weathers') }}