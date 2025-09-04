{% macro create_consume_prob_agg_functions() %}

    {% if target.name == 'spark' or target.name == 'databricks' %}
        select 1;
    {%- elif target.name == 'postgres' -%}
        create or replace function consume_prob_agg_results_inmemory(anyarray)
            returns setof record
            language plpgsql
            immutable
            as $$ begin
                return query select res.*
                from unnest($1) as res;
            end $$;

        create or replace function consume_prob_agg_results_temptable(results_table text)
            returns setof record
            language plpgsql
            volatile
            as $$ begin
                return query execute format('select * from %I', results_table);

                execute format('drop table %I', results_table);
                return;
            end $$;
    {%- endif -%}

{% endmacro %}

{% macro consume_prob_agg_results(field) %}

    {% if target.name == 'spark' or target.name == 'databricks' %}
        select 1;
    {%- elif target.name == 'postgres' -%}
        {%- if var('postgres_prob_agg_type') == 'inmemory' -%}
            consume_prob_agg_results_inmemory({{ field }})
        {%- elif var('postgres_prob_agg_type') == 'temptable' -%}
            consume_prob_agg_results_temptable({{ field }})
        {%- endif -%}
    {%- endif -%}

{% endmacro %}

{% macro create_prob_count_udaf() %}

    {% if target.name == 'spark' or target.name == 'databricks' %}
        create or replace function prob_count as 'com.doubtless.spark.hive.HiveProbCountGenericUDAF';
    {%- elif target.name == 'postgres' -%}
        {%- if var('postgres_prob_agg_type') == 'inmemory' -%}
            {{ create_postgres_prob_count_inmemory() }}
        {%- elif var('postgres_prob_agg_type') == 'temptable' -%}
            {{ create_postgres_prob_count_temptable() }}
        {%- endif -%}
    {% endif %}

{% endmacro %}

{% macro create_prob_sum_udaf() %}

    {% if target.name == 'spark' or target.name == 'databricks' %}
        create or replace function prob_sum as 'com.doubtless.spark.hive.HiveProbSumGenericUDAF';
    {% elif target.name == 'postgres' %}
        {%- if var('postgres_prob_agg_type') == 'inmemory' -%}
            {{ create_postgres_prob_sum_inmemory() }}
        {%- elif var('postgres_prob_agg_type') == 'temptable' -%}
            {{ create_postgres_prob_sum_temptable() }}
        {%- endif -%}
    {% endif %}

{% endmacro %}

{% macro create_prob_min_udaf() %}

    {% if target.name == 'spark' or target.name == 'databricks' %}
        create or replace function prob_min as 'com.doubtless.spark.hive.HiveProbMinGenericUDAF';
    {% elif target.name == 'postgres' %}
        {%- if var('postgres_prob_agg_type') == 'inmemory' -%}
            {{ create_postgres_prob_min_inmemory() }}
        {%- elif var('postgres_prob_agg_type') == 'temptable' -%}
            {{ create_postgres_prob_min_temptable() }}
        {%- endif -%}
    {% endif %}

{% endmacro %}

{% macro create_prob_max_udaf() %}

    {% if target.name == 'spark' or target.name == 'databricks' %}
        create or replace function prob_max as 'com.doubtless.spark.hive.HiveProbMaxGenericUDAF';
    {% elif target.name == 'postgres' %}
        {%- if var('postgres_prob_agg_type') == 'inmemory' -%}
            {{ create_postgres_prob_max_inmemory() }}
        {%- elif var('postgres_prob_agg_type') == 'temptable' -%}
            {{ create_postgres_prob_max_temptable() }}
        {%- endif -%}
    {% endif %}

{% endmacro %}

{% macro create_prob_avg_udaf() %}

    {% if target.name == 'spark' or target.name == 'databricks' %}
        create or replace function prob_avg as 'com.doubtless.spark.hive.HiveProbAvgGenericUDAF';
    {% elif target.name == 'postgres' %}
        {%- if var('postgres_prob_agg_type') == 'inmemory' -%}
            {{ create_postgres_prob_avg_inmemory() }}
        {%- elif var('postgres_prob_agg_type') == 'temptable' -%}
            {{ create_postgres_prob_avg_temptable() }}
        {%- endif -%}
    {% endif %}

{% endmacro %}
