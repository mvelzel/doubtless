with wdc_english_offers as (

    select * from {{ ref('stg_wdc__english_offers') }}

),

offers_exploded_properties as (

    select
        url,
        nodeID,
        property
    from wdc_english_offers
    lateral view explode(`schema.org_properties`) as property

),

offers_unnested_properties as (

    select
        url,
        nodeID,
        max(property.`/availability`) as property_availability,
        max(property.`/brand`) as property_brand,
        max(property.`/description`) as property_description,
        max(property.`/image`) as property_image,
        max(property.`/manufacturer`) as property_manufacturer,
        max(property.`/name`) as property_name,
        max(property.`/price`) as property_price,
        max(property.`/priceCurrency`) as property_price_currency,
        max(property.`/title`) as property_title
    from offers_exploded_properties
    group by url, nodeID

),

offers_exploded_identifiers as (

    select
        url,
        nodeID,
        identifier
    from wdc_english_offers
    lateral view explode(`identifiers`) as identifier

),

offers_unnested_identifiers as (

    select
        url,
        nodeID,
        max(identifier.`/gtin12`) as identifier_gtin12,
        max(identifier.`/gtin13`) as identifier_gtin13,
        max(identifier.`/gtin14`) as identifier_gtin14,
        max(identifier.`/gtin8`) as identifier_gtin8,
        max(identifier.`/identifier`) as identifier_identifier,
        max(identifier.`/mpn`) as identifier_mpn,
        max(identifier.`/productID`) as identifier_product_id,
        max(identifier.`/sku`) as identifier_sku
    from offers_exploded_identifiers
    group by url, nodeID

),

offers_unnested_combined as (

    select
        offer.url,
        offer.nodeID as node_id,
        offer.cluster_id,
        identifiers.identifier_gtin12,
        identifiers.identifier_gtin13,
        identifiers.identifier_gtin14,
        identifiers.identifier_gtin8,
        identifiers.identifier_identifier,
        identifiers.identifier_mpn,
        identifiers.identifier_product_id,
        identifiers.identifier_sku,
        properties.property_availability,
        properties.property_brand,
        properties.property_description,
        properties.property_image,
        properties.property_manufacturer,
        properties.property_name,
        properties.property_price,
        properties.property_price_currency,
        properties.property_title
    from wdc_english_offers as offer
    left join offers_unnested_identifiers as identifiers
    on offer.url = identifiers.url
    and offer.nodeID = identifiers.nodeID
    left join offers_unnested_properties as properties
    on offer.url = properties.url
    and offer.nodeID = properties.nodeID

)

select * from offers_unnested_combined
