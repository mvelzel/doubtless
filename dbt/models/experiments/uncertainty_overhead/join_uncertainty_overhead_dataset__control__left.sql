with gb_dataset as (

    select * from {{ ref('stg_dummy__gigabyte_dataset') }}

),

experiment_dataset as (

    select *
    from gb_dataset
    where row_num <= 100

)

select * from experiment_dataset
