{% macro create_prob_dict_udf() %}

    {% if target.name == 'spark' or target.name == 'databricks' %}
        create or replace function dictionary as 'com.doubtless.spark.hive.HiveProbDict';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}

{% macro create_prob_dict_to_string_udf() %}

    {% if target.name == 'spark' or target.name == 'databricks' %}
        create or replace function prob_dict_to_string as 'com.doubtless.spark.hive.HiveProbDictToString';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}
