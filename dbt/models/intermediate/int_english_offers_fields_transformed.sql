with english_offers_unnested as (

    select * from {{ ref('int_english_offers_unnested') }}

),

offers_transformed as (

    select
        url,
        node_id,
        cast(cluster_id as int),
        {{ remove_surrounding_brackets('identifier_gtin8') }} as identifier_gtin8,
        {{ remove_surrounding_brackets('identifier_gtin12') }} as identifier_gtin12,
        {{ remove_surrounding_brackets('identifier_gtin13') }} as identifier_gtin13,
        {{ remove_surrounding_brackets('identifier_gtin14') }} as identifier_gtin14,
        {{ remove_surrounding_brackets('identifier_identifier') }} as identifier_identifier,
        {{ remove_surrounding_brackets('identifier_mpn') }} as identifier_mpn,
        {{ remove_surrounding_brackets('identifier_product_id') }} as identifier_product_id,
        {{ remove_surrounding_brackets('identifier_sku') }} as identifier_sku,
        {{ remove_surrounding_brackets('property_availability') }} as property_availability,
        {{ remove_surrounding_brackets('property_brand') }} as property_brand,
        {{ remove_surrounding_brackets('property_description') }} as property_description,
        {{ remove_surrounding_brackets('property_image') }} as property_image,
        {{ remove_surrounding_brackets('property_manufacturer') }} as property_manufacturer,
        {{ remove_surrounding_brackets('property_name') }} as property_name,
        {{ remove_surrounding_brackets('property_price') }} as property_price,
        {{ remove_surrounding_brackets('property_price_currency') }} as property_price_currency,
        {{ remove_surrounding_brackets('property_title') }} as property_title
    from english_offers_unnested

)

select * from offers_transformed
