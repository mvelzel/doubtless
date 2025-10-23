{%- macro run_name_price_matching_probability_calculation_experiment(cluster_size=none, data_size=none) -%}

    {%- call statement('experiment', fetch_result=False) -%}

    select
        data.*,
        prob(dict.dictionary, data.sentence) as probability
    from
        {{ ref('english_offers_matching_dataset') }} as data,
        {{ ref('english_offers_matching_probability_dictionary') }} as dict
    {% if cluster_size is not none %}
    where data.cluster_size = {{ cluster_size }}
    {% endif %}
    {% if data_size is not none %}
    limit {{ data_size }}
    {% endif %}

    {%- endcall -%}

{%- endmacro -%}
