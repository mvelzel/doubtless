with source as (

    {% if target.name == 'spark' -%}

    select * from {{ source('raw', 'raw_wdc') }}

    {% elif target.name == 'postgres' -%}

    select
        json_data->>'cluster_id' as cluster_id,
        json_data->'identifiers' as identifiers,
        json_data->>'nodeID' as nodeID,
        json_data->>'parent_NodeID' as parent_NodeID,
        json_data->'parent_schema.org_properties' as "parent_schema.org_properties",
        json_data->>'relationToParent' as relationToParent,
        json_data->'schema.org_properties' as "schema.org_properties",
        json_data->>'url' as url
    from {{ source('raw', 'raw_wdc') }}

    {% endif -%}

)

select * from source
