{{ config(materialized='table') }}

select
    row_number() over (order by discipline, season)     as sport_id,
    discipline,
    season,
    contested
from (
    select discipline, 'Summer' as season, contested from {{ ref('stg_summer_sports') }}
    union all
    select discipline, 'Winter' as season, contested from {{ ref('stg_winter_sports') }}
    union all
    select discipline, 'Summer' as season, contested from {{ ref('stg_disc_summer_sports') }}
)