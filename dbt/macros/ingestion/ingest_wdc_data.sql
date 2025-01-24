{% macro ingest_wdc_data() %}
    create table if not exists raw.raw_wdc using json location "/Users/mvelzel/offers_english.json";
{% endmacro %}
