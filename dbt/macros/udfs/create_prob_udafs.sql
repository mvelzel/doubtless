{% macro create_prob_count_udaf() %}

    {% if target.name == 'spark' %}
        create or replace function prob_count as 'com.doubtless.spark.hive.HiveProbCountUDAF';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}

{% macro create_prob_sum_udaf() %}

    {% if target.name == 'spark' %}
        create or replace function prob_sum as 'com.doubtless.spark.hive.HiveProbSumUDAF';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}
