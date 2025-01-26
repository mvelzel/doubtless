{% macro create_prob_dict_udf() %}
    create or replace function prob_dict as 'com.doubtless.spark.hive.HiveProbDict';
{% endmacro %}

{% macro create_prob_dict_to_string_udf() %}
    create or replace function prob_dict_to_string as 'com.doubtless.spark.hive.HiveProbDictToString';
{% endmacro %}
