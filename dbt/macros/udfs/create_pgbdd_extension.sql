{% macro create_pgbdd_extension() %}

    {% if target.name == 'postgres' %}
        create extension if not exists pgbdd;
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}
