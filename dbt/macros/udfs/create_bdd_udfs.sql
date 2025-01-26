{% macro create_bdd_udf() %}
    
    {% if target.name == 'spark' %}
        create or replace function bdd as 'com.doubtless.spark.hive.HiveBDD';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}

{% macro create_bdd_to_string_udf() %}
    
    {% if target.name == 'spark' %}
        create or replace function bdd_to_string as 'com.doubtless.spark.hive.HiveBDDToString';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}

{% macro create_bdd_and_udf() %}
    
    {% if target.name == 'spark' %}
        create or replace function bdd_and as 'com.doubtless.spark.hive.HiveBDDAnd';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}

{% macro create_bdd_or_udf() %}
    
    {% if target.name == 'spark' %}
        create or replace function bdd as 'com.doubtless.spark.hive.HiveBDD';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}

{% macro create_bdd_not_udf() %}
    
    {% if target.name == 'spark' %}
        create or replace function bdd_not as 'com.doubtless.spark.hive.HiveBDDNot';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}
