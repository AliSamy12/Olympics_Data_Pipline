{{ config(materialized='table') }}

select
    g.year,
    h.host_id,
    g.season,
    g.n_medal_events                                                        as number_of_medal_events,
    g.gold_count                                                            as gold,
    g.silver_count                                                          as silver,
    g.bronze_count                                                          as bronze,
    g.total_medals                                                          as total,
    ma.athlete_id                                                           as most_medals_athlete_id,
    mg.athlete_id                                                           as most_gold_athlete_id
from (
    select *, 'Summer' as season from {{ ref('stg_summer_games') }}
    union all
    select *, 'Winter' as season from {{ ref('stg_winter_games') }}
) g
left join {{ ref('dim_host') }} h
    on trim(split_part(g.host, ',', 1)) = h.city
    and trim(split_part(g.host, ',', 2)) = h.country
left join {{ ref('dim_athlete') }} ma
    on trim(regexp_replace(g.athlete_most_medals, '\s*\(.*', '')) = ma.name
left join {{ ref('dim_athlete') }} mg
    on trim(regexp_replace(g.athlete_most_gold, '\s*\(.*', '')) = mg.name