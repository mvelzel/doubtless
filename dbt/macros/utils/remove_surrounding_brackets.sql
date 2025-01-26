{%- macro remove_surrounding_brackets(column_name) -%}
    {%- if target.name == 'spark' -%}
        regexp_replace({{ column_name }}, '^\s*\\[?\s*|\s*\\]?\s*$', '')
    {%- elif target.name == 'postgres' -%}
        regexp_replace({{ column_name }}, '^\s*\[?\s*|\s*\]?\s*$', '', 'g')
    {%- endif -%}
{%- endmacro -%}
