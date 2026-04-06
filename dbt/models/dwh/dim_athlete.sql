{{ config(materialized='table') }}

with base as (
    select
        trim(regexp_replace(athlete, '\s*\(.*', '')) as name,
        regexp_extract(athlete, '\(\s*([A-Z]{2,3})\s*\)', 1) as country
    from (
        select athlete_most_medals as athlete from {{ ref('stg_summer_games') }}
        union
        select athlete_most_gold   as athlete from {{ ref('stg_summer_games') }}
        union
        select athlete_most_medals as athlete from {{ ref('stg_winter_games') }}
        union
        select athlete_most_gold   as athlete from {{ ref('stg_winter_games') }}
        union
        select athlete_most_medals as athlete from {{ ref('stg_summer_sports') }}
        union
        select athlete_most_gold   as athlete from {{ ref('stg_summer_sports') }}
        union
        select athlete_most_medals as athlete from {{ ref('stg_winter_sports') }}
        union
        select athlete_most_gold   as athlete from {{ ref('stg_winter_sports') }}
    )
    where athlete is not null and trim(athlete) != ''
)

select
    row_number() over (order by name) as athlete_id,
    name,
    country
from base