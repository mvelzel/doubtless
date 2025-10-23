with gb_dataset as (

    select * from {{ ref('stg_dummy__gigabyte_dataset') }}

)

select * from gb_dataset
