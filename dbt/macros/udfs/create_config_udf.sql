{% macro create_config_udf() %}
    
    {% if target.name == 'spark' or target.name == 'databricks' %}
        create or replace function config as 'com.doubtless.spark.hive.HiveConfig';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}
