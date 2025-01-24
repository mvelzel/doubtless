{{ config(materialized='table') }}

with source_data as (

    select 1 as group, cast(1.5 as double) as num, bdd('x=1') as bdd
    union all
    select 1 as group, cast(2.0 as double) as num, bdd('x=1') as bdd
    union all
    select 1 as group, cast(0.5 as double) as num, bdd('y=1') as bdd
    union all
    select 1 as group, cast(1.5 as double) as num, bdd('y=2') as bdd

)

select *
from source_data
