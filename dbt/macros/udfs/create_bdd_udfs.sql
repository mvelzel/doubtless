{% macro create_bdd_udf() %}
    create or replace function bdd as 'com.doubtless.spark.hive.HiveBDD';
{% endmacro %}

{% macro create_bdd_to_string_udf() %}
    create or replace function bdd_to_string as 'com.doubtless.spark.hive.HiveBDDToString';
{% endmacro %}

{% macro create_bdd_and_udf() %}
    create or replace function bdd_and as 'com.doubtless.spark.hive.HiveBDDAnd';
{% endmacro %}

{% macro create_bdd_or_udf() %}
    create or replace function bdd as 'com.doubtless.spark.hive.HiveBDD';
{% endmacro %}

{% macro create_bdd_not_udf() %}
    create or replace function bdd_not as 'com.doubtless.spark.hive.HiveBDDNot';
{% endmacro %}
