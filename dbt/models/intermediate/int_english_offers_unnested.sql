with wdc_english_offers as (

    select * from {{ ref('stg_wdc__english_offers') }}

),

offers_exploded_properties as (

    select
        offer.url,
        offer.nodeID,
        {% if target.name == 'spark' or target.name == 'databricks' -%}
        property
        {% elif target.name == 'postgres' -%}
        json.value as property
        {% endif -%}
    from wdc_english_offers as offer
    {% if target.name == 'spark' or target.name == 'databricks' -%}
    lateral view explode(offer.`schema.org_properties`) as property
    {% elif target.name == 'postgres' -%}
    cross join jsonb_array_elements(offer."schema.org_properties") as json
    {% endif -%}

),

offers_unnested_properties as (

    select
        url,
        nodeID,
        max({{ get_from_struct('property', '/availability') }}) as property_availability,
        max({{ get_from_struct('property', '/brand') }}) as property_brand,
        max({{ get_from_struct('property', '/description') }}) as property_description,
        max({{ get_from_struct('property', '/image') }}) as property_image,
        max({{ get_from_struct('property', '/manufacturer') }}) as property_manufacturer,
        max({{ get_from_struct('property', '/name') }}) as property_name,
        max({{ get_from_struct('property', '/price') }}) as property_price,
        max({{ get_from_struct('property', '/priceCurrency') }}) as property_price_currency,
        max({{ get_from_struct('property', '/title') }}) as property_title
    from offers_exploded_properties
    group by url, nodeID

),

offers_exploded_identifiers as (

    select
        offer.url,
        offer.nodeID,
        {% if target.name == 'spark' or target.name == 'databricks' -%}
        identifier
        {% elif target.name == 'postgres' -%}
        json.value as identifier
        {% endif -%}
    from wdc_english_offers as offer
    {% if target.name == 'spark' or target.name == 'databricks' -%}
    lateral view explode(offer.`identifiers`) as identifier
    {% elif target.name == 'postgres' -%}
    cross join jsonb_array_elements(offer."identifiers") as json
    {% endif -%}

),

offers_unnested_identifiers as (

    select
        url,
        nodeID,
        max({{ get_from_struct('identifier', '/gtin12') }}) as identifier_gtin12,
        max({{ get_from_struct('identifier', '/gtin13') }}) as identifier_gtin13,
        max({{ get_from_struct('identifier', '/gtin14') }}) as identifier_gtin14,
        max({{ get_from_struct('identifier', '/gtin8') }}) as identifier_gtin8,
        max({{ get_from_struct('identifier', '/identifier') }}) as identifier_identifier,
        max({{ get_from_struct('identifier', '/mpn') }}) as identifier_mpn,
        max({{ get_from_struct('identifier', '/productID') }}) as identifier_product_id,
        max({{ get_from_struct('identifier', '/sku') }}) as identifier_sku
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
