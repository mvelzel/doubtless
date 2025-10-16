with sized_bdds as (

    select * from {{ ref('int_sized_bdds_decoded') }}

),

gb_dataset as (

    select * from {{ ref('stg_dummy__gigabyte_dataset') }}

),

experiment_dataset as (

    select gb.*, sized.bdd
    from gb_dataset as gb
    left join sized_bdds as sized
    on sized.size = 1000000
    and sized.is_alt = true
    where gb.row_num <= 100

)

select * from experiment_dataset
