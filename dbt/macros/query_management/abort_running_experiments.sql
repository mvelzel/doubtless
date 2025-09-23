{%- macro abort_running_experiments() -%}

    {% set sql %}
    {% if target.name == 'postgres' %}

    select pg_cancel_backend(pid)
    from pg_stat_activity
    where state = 'active'
    and application_name = 'dbt'
    and pid <> pg_backend_pid();

    {% elif target.name == 'spark' or target.name == 'databricks' %}

    select 1;

    {% endif %}
    {% endset %}

    {% do run_query(sql) %}

{%- endmacro -%}
