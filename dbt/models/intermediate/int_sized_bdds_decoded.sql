{{
    config(
        materialized='table'
    )
}}

with sized_base64 as (

    select * from {{ ref('stg_dummy__sized_bdds') }}

),

base64_decoded as (

    {% if target.name == 'spark' or target.name == 'databricks' -%}
    select
        size,
        unbase64(base64) as bdd
    from sized_base64
    {% else -%}
    -- TODO Implement this for Postgres?
    select
        100 as size
        bdd('x=1') as bdd
    {% endif -%}

)

select * from base64_decoded
