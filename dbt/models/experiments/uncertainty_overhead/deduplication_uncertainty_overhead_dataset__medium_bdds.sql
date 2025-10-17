with sized_bdds as (

    select * from {{ ref('int_sized_bdds_decoded') }}

),

gb_dataset as (

    select * from {{ ref('stg_dummy__gigabyte_dataset') }}

),

base_dataset as (

    select gb.*, sized.bdd
    from gb_dataset as gb
    left join sized_bdds as sized
    on sized.size = 100000
    and (
        sized.is_alt = false
        and gb.row_num % 2 = 0)
    ) or (
        sized.is_alt = true
        and gb.row_num % 2 = 1
    )
    where gb.row_num <= 100

),

experiment_dataset as (

    select base.*
    from explode(sequence(1, 100)), base_dataset as base

)

select * from experiment_dataset
