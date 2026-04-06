{{ config(materialized='table') }}

select
    row_number() over (order by host)           as host_id,
    trim(split_part(host, ',', 1))              as city,
    trim(split_part(host, ',', 2))              as country
from (
    select host from {{ ref('stg_summer_games') }}
    union
    select host from {{ ref('stg_winter_games') }}
)