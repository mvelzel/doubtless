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
    on sized.size = 1000
    and sized.is_alt = (gb.row_num % 2 = 1)
    where gb.row_num <= 100

),

experiment_dataset as (

    select base.*
    {% if target.name == 'spark' or target.name == 'databricks' -%}
    from explode(sequence(1, 100))
    {% else -%}
    from generate_series(1, 100) as col
    {% endif -%}, base_dataset as base

)

select * from experiment_dataset
