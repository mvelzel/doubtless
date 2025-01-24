{% macro create_bdd_agg_and_udaf() %}
    create or replace function bdd_agg_and as 'com.doubtless.spark.hive.HiveBDDAggAndUDAF';
{% endmacro %}
