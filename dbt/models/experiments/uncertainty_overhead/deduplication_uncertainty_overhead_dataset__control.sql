with gb_dataset as (

    select * from {{ ref('stg_dummy__gigabyte_dataset') }}

),

base_dataset as (

    select *
    from gb_dataset
    where row_num <= 100

),

experiment_dataset as (

    select base.*
    from explode(sequence(1, 100)), base_dataset as base

)

select * from experiment_dataset
