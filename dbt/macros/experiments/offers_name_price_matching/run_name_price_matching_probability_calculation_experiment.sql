{%- macro run_name_price_matching_probability_calculation_experiment(cluster_size=none, data_size=none) -%}

    {%- call statement('experiment', fetch_result=False) -%}

    with offers_with_sentences as (

        select *
        from {{ ref('english_offers_matching_dataset') }}
        {% if cluster_size is not none %}
        where cluster_size = {{ cluster_size }}
        {% endif %}
        {% if data_size is not none %}
        limit {{ data_size }}
        {% endif %}

    ),

    probability_dictionary as (

        select dictionary
        from {{ ref('english_offers_matching_probability_dictionary') }}
        limit 1

    )

    select
        data.*,
        prob(dict.dictionary, data.sentence) as probability
    from
        offers_with_sentences as data,
        probability_dictionary as dict

    {%- endcall -%}

{%- endmacro -%}
