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
    {% if target.name == 'spark' or target.name == 'databricks' -%}
    from explode(sequence(1, 100))
    {% else -%}
    from generate_series(1, 100) as col
    {% endif -%}, base_dataset as base

)

select * from experiment_dataset
