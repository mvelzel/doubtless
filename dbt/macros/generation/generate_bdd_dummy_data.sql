{%- macro generate_bdd_dummy_data(
    groups,
    group_variables,
    variable_alternatives,
    group_size=none,
    include_random_numbers=False,
    experiment_name='experiment'
) -%}
    {%- set group_size = group_size or group_variables * variable_alternatives -%}
    {%- set start_seed = 0 -%}

    {%- for group in range(groups) -%}
    {%- for i in range(group_size) -%}
    {%- set var_name = 'g' ~ (i // variable_alternatives) -%}
    {% set alternative = '' ~ (i % variable_alternatives) %}

    {%- set seed_sql -%}
    select setseed({{ (start_seed + i / group_size) + (group * group_size) }})
    {%- endset -%}

    {%- if include_random_numbers and target.name == 'postgres' %}
    {%- do run_query(seed_sql) -%}
    {%- endif %}

    (
        '{{ experiment_name }}',
        {{ group }},
        {% if include_random_numbers -%}
        {{ (range(100) | random) / 100 }},
        {% endif -%}
        {% if i >= group_variables * variable_alternatives -%}
        bdd('g1=0'),
        'g1',
        0,
        {% else -%}
        bdd('{{ var_name ~ "=" ~ alternative }}'),
        '{{ var_name }}',
        {{ alternative }}
        {% endif -%}
    )
    {%- if not loop.last %}
    ,
    {%- endif %}
    {%- endfor -%}
    {%- if not loop.last %}
    ,
    {%- endif %}
    {%- endfor -%}

{%- endmacro -%}
