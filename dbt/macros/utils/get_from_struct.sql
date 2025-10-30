{%- macro get_from_struct(column_name, struct_property) -%}
    {%- if target.name == 'spark' or target.name == 'databricks' -%}
        `{{ column_name }}`.`{{ struct_property }}`
    {%- elif target.name == 'postgres' -%}
        "{{ column_name }}"->>'{{ struct_property }}'
    {%- endif -%}
{%- endmacro -%}
