{{ config(materialized='table') }}

select
    ds.sport_id,
    s.season,
    s.contested,
    s.n_olympics                                                            as number_of_olympics,
    s.n_medal_events                                                        as number_of_medal_events,
    s.gold_count                                                            as gold,
    s.silver_count                                                          as silver,
    s.bronze_count                                                          as bronze,
    s.total_medals                                                          as total,
    ma.athlete_id                                                           as most_medals_athlete_id,
    mg.athlete_id                                                           as most_gold_athlete_id
from (
    select *, 'Summer' as season, n_medal_events_2024 as n_medal_events from {{ ref('stg_summer_sports') }}
    union all
    select *, 'Winter' as season, n_medal_events_2026 as n_medal_events from {{ ref('stg_winter_sports') }}
) s
left join {{ ref('dim_sport') }} ds
    on s.discipline = ds.discipline
    and s.season = ds.season
left join {{ ref('dim_athlete') }} ma
    on trim(regexp_replace(s.athlete_most_medals, '\s*\(.*', '')) = ma.name
left join {{ ref('dim_athlete') }} mg
    on trim(regexp_replace(s.athlete_most_gold, '\s*\(.*', '')) = mg.name