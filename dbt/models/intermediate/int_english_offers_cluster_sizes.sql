{{
    config(materialized='table')
}}


with english_offers as (

    select * from {{ ref('int_english_offers_fields_transformed') }}

),

cluster_sizes as (

    select
        cluster_id,
        count(*) as cluster_size
    from english_offers
    group by cluster_id

)

select * from cluster_sizes
