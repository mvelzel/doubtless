{{ config(materialized='view') }}

with source_data as (

    select * from {{ source('raw', 'raw_wdc') }}

)

select * from source_data
