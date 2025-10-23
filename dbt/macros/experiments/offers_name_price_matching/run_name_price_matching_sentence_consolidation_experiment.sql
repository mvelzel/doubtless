{%- macro run_name_price_matching_sentence_consolidation_experiment(cluster_size=none, data_size=none) -%}

    {%- call statement('experiment', fetch_result=False) -%}

    with offers_with_sentences as (

        select
            cluster_id,
            property_name,
            name_matching_sentence,
            property_price,
            price_matching_sentence
        from {{ ref('int_english_offers_matching_sentence_enriched') }}
        {% if cluster_size is not none %}
        where cluster_size = {{ cluster_size }}
        {% endif %}
        {% if data_size is not none %}
        limit {{ data_size }}
        {% endif %}

    ),

    offer_names as (

        select
            cluster_id,
            property_name,
            name_matching_sentence,
            row_number() over (partition by cluster_id, property_name order by cluster_id) as row_number
        from offers_with_sentences
        where property_name is not null

    ),

    offer_prices as (

        select
            cluster_id,
            property_price,
            price_matching_sentence,
            row_number() over (partition by cluster_id, property_price order by cluster_id) as row_number
        from offers_with_sentences
        where property_price is not null

    ),

    offer_names_and_prices as (

        select
            coalesce(name.cluster_id, price.cluster_id) as cluster_id,
            name.property_name,
            price.property_price,
            case
                when name.name_matching_sentence is null then price.price_matching_sentence
                when price.price_matching_sentence is null then name.name_matching_sentence
                else _and(name.name_matching_sentence, price.price_matching_sentence)
            end as sentence
        from offer_names as name
        full join offer_prices as price
        on name.cluster_id = price.cluster_id
        and name.row_number = price.row_number
        where name.row_number = 1
        and price.row_number = 1

    )

    select * from offer_names_and_prices

    {%- endcall -%}

{%- endmacro -%}
