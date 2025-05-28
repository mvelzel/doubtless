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

    select
        '{{ experiment_name }}' as experiment_name,
        {{ group }} as group,
        {% if include_random_numbers -%}
        {% if target.name == 'spark' -%}
        rand({{ (start_seed + i) + (group * group_size) }}) as number,
        {% elif target.name == 'postgres' -%}
        random() as number,
        {% endif -%}
        {% endif -%}
        {% if i >= group_variables * variable_alternatives -%}
        bdd('g1=0') as sentence,
        'g1' as variable,
        0 as alternative,
        {% else -%}
        bdd('{{ var_name ~ "=" ~ alternative }}') as sentence,
        '{{ var_name }}' as variable,
        {{ alternative }} as alternative
        {% endif -%}
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor -%}
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor -%}

{%- endmacro -%}
