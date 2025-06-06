{% macro create_bdd_agg_and_udaf() %}
    
    {% if target.name == 'spark' %}
        create or replace function bdd_agg_and as 'com.doubtless.spark.hive.HiveBDDAggAndUDAF';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}

{% macro create_bdd_agg_or_udaf() %}

    {% if target.name == 'spark' %}
        create or replace function bdd_agg_or as 'com.doubtless.spark.hive.HiveBDDAggOrUDAF';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}
