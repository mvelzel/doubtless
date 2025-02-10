{%- macro generate_bdd_dummy_data(
    groups,
    group_variables,
    variable_alternatives,
    group_size=none,
    include_random_numbers=False
) -%}
    {%- set group_size = group_size or group_variables * variable_alternatives -%}

    {%- for group in range(groups) -%}
    {%- for i in range(group_size) -%}
    {%- set var_name = 'g' ~ (i // variable_alternatives) -%}
    {% set alternative = '' ~ (i % variable_alternatives) %}

    select
        {{ group }} as group,
        {% if i >= group_variables * variable_alternatives -%}
        bdd('g1=0') as sentence,
        'g1' as variable,
        0 as alternative
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
