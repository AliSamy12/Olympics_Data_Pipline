{{ config(materialized='table') }}

with source as (
    select * from {{ ref('Summer_Olympic_sports') }}
),

cleaned as (
    select
        trim(discipline_link_to_medalists_list)                                     as discipline,
        trim(contested)                                                             as contested,
        olympics_up_to_conclusion_of_2024_                                          as n_olympics,
        medal_events_in_2024_                                                       as n_medal_events_2024,
        "1st_place_gold_medalists"                                                  as gold_count,
        "2nd_place_silver_medalists"                                                as silver_count,
        "3rd_place_bronze_medalists"                                                as bronze_count,
        total                                                                       as total_medals,
        trim(athletes_with_the_most_medals_goldsilverbronze)                        as athlete_most_medals,
        trim(athletes_with_the_most_gold_medals)                                    as athlete_most_gold
    from source
)

select * from cleaned