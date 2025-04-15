{{
    config(
        materialized="incremental",
        partition_by=["experiment_name"],
        incremental_strategy="insert_overwrite"
    )
}}

with dummy_data as (

    {%- for variable_count in range(11, 13) -%}
    {%- for alternative_count in range(1, 13) -%}
    {{ generate_bdd_dummy_data(
        groups=1,
        group_variables=variable_count,
        variable_alternatives=alternative_count,
        experiment_name='prob_count_' ~ variable_count ~ '_' ~ alternative_count
    ) }}
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor -%}
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor -%}

)

select * from dummy_data
