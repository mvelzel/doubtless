{% macro create_prob_count_udaf() %}
    create or replace function prob_count as 'com.doubtless.spark.hive.HiveProbCountUDAF';
{% endmacro %}

{% macro create_prob_sum_udaf() %}
    create or replace function prob_sum as 'com.doubtless.spark.hive.HiveProbSumUDAF';
{% endmacro %}
