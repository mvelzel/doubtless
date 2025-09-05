with dummy_data as (

    select * from (
        values
        {%- for variable_count in range(1, 10) -%}
        {%- for alternative_count in range(1, 10) -%}
        {{ generate_bdd_dummy_data(
            groups=1,
            group_variables=variable_count,
            variable_alternatives=alternative_count,
            experiment_name='prob_avg_' ~ variable_count ~ '_' ~ alternative_count,
            include_random_numbers=True
        ) }}
        {%- if not loop.last %}
        ,
        {%- endif %}
        {%- endfor -%}
        {%- if not loop.last %}
        ,
        {%- endif %}
        {%- endfor -%}
    ) as t (experiment_name, group_index, number, sentence, variable, alternative)

)

select * from dummy_data
