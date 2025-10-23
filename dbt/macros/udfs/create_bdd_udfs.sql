{% macro create_bdd_udf() %}
    
    {% if target.name == 'spark' or target.name == 'databricks' %}
        create or replace function bdd as 'com.doubtless.spark.hive.HiveBDD';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}

{% macro create_bdd_to_string_udf() %}
    
    {% if target.name == 'spark' or target.name == 'databricks' %}
        create or replace function bdd_to_string as 'com.doubtless.spark.hive.HiveBDDToString';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}

{% macro create_bdd_and_udf() %}
    
    {% if target.name == 'spark' or target.name == 'databricks' %}
        create or replace function _and as 'com.doubtless.spark.hive.HiveBDDAnd';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}

{% macro create_bdd_or_udf() %}
    
    {% if target.name == 'spark' or target.name == 'databricks' %}
        create or replace function _or as 'com.doubtless.spark.hive.HiveBDDOr';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}

{% macro create_bdd_not_udf() %}
    
    {% if target.name == 'spark' or target.name == 'databricks' %}
        create or replace function _not as 'com.doubtless.spark.hive.HiveBDDNot';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}

{% macro create_bdd_equiv_udf() %}
    
    {% if target.name == 'spark' or target.name == 'databricks' %}
        create or replace function bdd_fast_equiv as 'com.doubtless.spark.hive.HiveBDDEquiv';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}

{% macro create_bdd_prob_udf() %}
    
    {% if target.name == 'spark' or target.name == 'databricks' %}
        create or replace function prob as 'com.doubtless.spark.hive.HiveBDDNot';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}

{% macro create_bdd_prune_and_udf() %}

    {% if target.name == 'postgres' %}
        create or replace function prune_and(this bdd, that bdd)
        returns bdd
        language plpgsql
        as $$
        declare res bdd;
        begin
            select (this & that) into res;

            if bdd_fast_equiv(res, bdd('0')) then
                return bdd('0');
            else
                return res;
            end if;
        end $$;
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}
