with sized_bdds as (

    select * from {{ ref('int_sized_bdds_decoded') }}

),

gb_dataset as (

    select
        row_num,
        {% for i in range(1, 21) %}
        column_{{ i }}
        {% if not loop.last %}
        ,
        {% endif %}
        {% endfor %}
    from {{ ref('stg_dummy__gigabyte_dataset') }}

),

experiment_dataset as (

    select gb.*, sized.bdd
    from gb_dataset as gb
    left join sized_bdds as sized
    on sized.size = 100000
    and sized.is_alt = false
    where gb.row_num <= 100

)

select * from experiment_dataset
