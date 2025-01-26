{% macro ingest_wdc_data() %}

    {% if target.name == 'spark' %}
        create table if not exists raw.raw_wdc using json location "/Users/mvelzel/offers_english.json";
    {% elif target.name == 'postgres' and execute %}
        {% set data_exists_statement %}
            select exists (
                select from information_schema.tables
                where table_schema = 'raw'
                and table_name = 'raw_wdc'
            )
        {% endset %}
        {% set relation_exists = dbt_utils.get_single_value(data_exists_statement, default=False) %}

        {% if not relation_exists %}
            create table raw.raw_wdc (json_data jsonb);
            copy raw.raw_wdc from '/Users/mvelzel/offers_english.json' csv quote e'\x01' delimiter e'\x02';
        {% else %}
            select 1;
        {% endif %}
    {% endif %}

{% endmacro %}
