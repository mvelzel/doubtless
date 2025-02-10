{%- macro refresh_spark_table() -%}

    {%- if target.name == 'spark' -%}
        refresh table {{ this }};
    {%- else -%}
        select 1;
    {%- endif -%}

{%- endmacro -%}
