{{ config(materialized='table') }}

with source as (
    select * from {{ ref('Winter_Olympic_Games') }}
),

cleaned as (
    select
        games::int                                                                  as year,
        trim(host)                                                                  as host,
        number_of_medal_events                                                      as n_medal_events,
        "1st_place_gold_medalists"                                                  as gold_count,
        "2nd_place_silver_medalists"                                                as silver_count,
        "3rd_place_bronze_medalists"                                                as bronze_count,
        total                                                                       as total_medals,
        trim(athletes_with_the_most_medals_goldsilverbronze)                        as athlete_most_medals,
        trim(athletes_with_the_most_gold_medals)                                    as athlete_most_gold
    from source
)

select * from cleaned