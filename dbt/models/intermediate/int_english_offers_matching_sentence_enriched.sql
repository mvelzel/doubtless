{{
    config(
        materialized='table',
        post_hook='{{ refresh_spark_table() }}'
    )
}}

with english_offers as (

    select * from {{ ref('int_english_offers_fields_transformed') }}

),

offers_cluster_name_counts as (

    select
        cluster_id,
        property_name,
        count(*) as name_count,
        sum(count(*)) over (partition by cluster_id) as total_name_count,
        row_number() over (partition by cluster_id order by property_name) as name_matching_variable_alternative
    from english_offers
    where property_name is not null
    group by cluster_id, property_name

),

offers_cluster_price_counts as (

    select
        cluster_id,
        property_price,
        count(*) as price_count,
        sum(count(*)) over (partition by cluster_id) as total_price_count,
        row_number() over (partition by cluster_id order by property_price) as price_matching_variable_alternative
    from english_offers
    where property_price is not null
    group by cluster_id, property_price

),

cluster_variable_names as (

    select
        cluster_id,
        row_number() over (order by cluster_id) as variable_name
    from english_offers
    group by cluster_id

),

offers_with_bdds as (

    select
        offer.*,
        concat('pn', cluster_variable_name.variable_name) as name_matching_variable_name,
        name_counts.name_matching_variable_alternative,
        case coalesce(name_counts.name_count, -1)
            when -1 then bdd('0')
            when name_counts.total_name_count then bdd('1')
            else bdd(concat(
                'pn',
                cluster_variable_name.variable_name,
                '=',
                name_counts.name_matching_variable_alternative
            ))
        end as name_matching_sentence,
        case when name_counts.cluster_id is not null
            then name_counts.name_count / name_counts.total_name_count
            else null
        end as name_matching_probability,
        concat('pp', cluster_variable_name.variable_name) as price_matching_variable_name,
        price_counts.price_matching_variable_alternative,
        case coalesce(price_counts.price_count, -1)
            when -1 then bdd('0')
            when price_counts.total_price_count then bdd('1')
            else bdd(concat(
                'pp',
                cluster_variable_name.variable_name,
                '=',
                price_counts.price_matching_variable_alternative
            ))
        end as price_matching_sentence,
        case when price_counts.cluster_id is not null
            then price_counts.price_count / price_counts.total_price_count
            else null
        end as price_matching_probability
    from english_offers as offer
    left join cluster_variable_names as cluster_variable_name
    on offer.cluster_id = cluster_variable_name.cluster_id
    left join offers_cluster_name_counts as name_counts
    on offer.cluster_id = name_counts.cluster_id
    and offer.property_name = name_counts.property_name
    left join offers_cluster_price_counts as price_counts
    on offer.cluster_id = price_counts.cluster_id
    and offer.property_price = price_counts.property_price

)

select * from offers_with_bdds
