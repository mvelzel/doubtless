{%- macro string_agg(column_name, separator) -%}
    {%- if target.name == 'spark' or target.name == 'databricks' -%}
        concat_ws('{{ separator }}', collect_list({{ column_name }}))
    {%- elif target.name == 'postgres' -%}
        string_agg({{ column_name }}, '{{ separator }}')
    {%- endif -%}
{%- endmacro -%}
