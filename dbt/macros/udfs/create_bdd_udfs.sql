{% macro create_bdd_udfs() %}

    {% do return([
        "create or replace function bdd as 'com.doubtless.spark.hive.HiveBDD';",
        "create or replace function bdd_to_string as 'com.doubtless.spark.hive.HiveBDDToString';",
        "create or replace function bdd_and as 'com.doubtless.spark.hive.HiveBDDAnd';",
        "create or replace function bdd_or as 'com.doubtless.spark.hive.HiveBDDOr';",
        "create or replace function bdd_not as 'com.doubtless.spark.hive.HiveBDDNot';"
    ]) %}

{% endmacro %}
