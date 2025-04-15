with dummy_data as (

    {%- for experiment_size in range(1, 13) -%}
    {{ generate_bdd_dummy_data(
        groups=1,
        group_variables=experiment_size,
        variable_alternatives=experiment_size,
        experiment_name='prob_sum_' ~ experiment_size,
        include_random_numbers=True
    ) }}
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor -%}

)

select * from dummy_data
