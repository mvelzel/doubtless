with gb_dataset as (

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

    select *
    from gb_dataset
    where row_num <= 100

)

select * from experiment_dataset
