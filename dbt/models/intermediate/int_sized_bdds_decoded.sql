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
        length(unbase64(base64)) as actual_size,
        is_alt,
        unbase64(base64) as bdd
    from sized_base64
    {% else -%}
    select
        size,
        length(decode(base64, 'base64')) as actual_size,
        is_alt as is_alt,
        decode(base64, 'base64')::bdd as bdd
    from sized_base64
    {% endif -%}

)

select * from base64_decoded
