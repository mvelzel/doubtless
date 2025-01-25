with source as (

    select * from {{ source('raw', 'raw_wdc') }}

)

select * from source
