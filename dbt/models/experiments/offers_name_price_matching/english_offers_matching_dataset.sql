with offers_with_sentences as (

    select
        cluster_id,
        property_name,
        name_matching_sentence,
        property_price,
        price_matching_sentence
    from {{ ref('int_english_offers_matching_sentence_enriched') }}

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

),

with_cluster_sizes as (

    select
        *,
        count(*) over (partition by cluster_id) as cluster_size
    from offer_names_and_prices

)

select * from with_cluster_sizes
