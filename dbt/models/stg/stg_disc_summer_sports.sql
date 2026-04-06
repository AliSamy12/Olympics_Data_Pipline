{{ config(materialized='table') }}

with source as (
    select * from {{ ref('Discontinued_summer_sports') }}
),

cleaned as (
    select
        trim(discipline)                                                            as discipline,
        trim(contested::varchar)                                                    as contested,
        number_of_olympics                                                          as n_olympics,
        "1st_place_gold_medalists"                                                  as gold_count,
        "2nd_place_silver_medalists"                                                as silver_count,
        "3rd_place_bronze_medalists"                                                as bronze_count,
        total                                                                       as total_medals
    from source
)

select * from cleaned