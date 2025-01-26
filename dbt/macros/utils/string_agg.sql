{%- macro string_agg(column_name, separator) -%}
    {%- if target.name == 'spark' -%}
        concat_ws('{{ separator }}', collect_list({{ column_name }}))
    {%- elif target.name == 'postgres' -%}
        string_agg({{ column_name }}, '{{ separator }}')
    {%- endif -%}
{%- endmacro -%}
