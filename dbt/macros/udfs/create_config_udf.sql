{% macro create_config_udf() %}
    
    {% if target.name == 'spark' %}
        create or replace function config as 'com.doubtless.spark.hive.HiveConfig';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}
