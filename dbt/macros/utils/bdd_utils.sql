{%- macro bdd_and(left_bdd, right_bdd) -%}
    {%- if target.name == 'spark' -%}
        bdd_and({{ left_bdd }}, {{ right_bdd }})
    {%- elif target.name == 'postgres' -%}
        _op_bdd('&', {{ left_bdd }}, {{ right_bdd }})
    {%- endif -%}
{%- endmacro -%}
