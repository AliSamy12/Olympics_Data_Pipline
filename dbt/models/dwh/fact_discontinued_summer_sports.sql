{{ config(materialized='table') }}

select
    ds.sport_id,
    s.contested,
    s.n_olympics                                                            as number_of_olympics,
    s.gold_count                                                            as gold,
    s.silver_count                                                          as silver,
    s.bronze_count                                                          as bronze,
    s.total_medals                                                          as total
from {{ ref('stg_disc_summer_sports') }} s
left join {{ ref('dim_sport') }} ds
    on s.discipline = ds.discipline
    and ds.season = 'Summer'