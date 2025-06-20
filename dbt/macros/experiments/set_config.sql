{%- macro set_config(config) -%}

    {% set sql %}
        drop table if exists experiments.experiments_config;
        create table experiments.experiments_config (
            config jsonb
        );
        insert into experiments.experiments_config (config)
        values ('{{ tojson(config) }}'::jsonb);
    {% endset %}

    {%- if target.name == 'postgres' -%}
        {% do run_query(sql) %}
    {%- endif -%}

{%- endmacro -%}
