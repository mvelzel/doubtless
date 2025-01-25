{%- macro remove_surrounding_brackets(column_name) -%}
    regexp_replace({{ column_name }}, '^\s*\\[?\s*|\s*\\]?\s*$', '')
{%- endmacro -%}
