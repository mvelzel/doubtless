{{ config(materialized='table') }}

with source_data as (

    select 1 as group, cast(1.5 as double) as num, bdd('x=1') as bdd
    union all
    select 1 as group, cast(2.0 as double) as num, bdd('x=1') as bdd
    union all
    select 1 as group, cast(0.5 as double) as num, bdd('y=1') as bdd
    union all
    select 1 as group, cast(1.5 as double) as num, bdd('x=2') as bdd
    union all
    select 1 as group, cast(2.0 as double) as num, bdd('h=4') as bdd
    union all
    select 1 as group, cast(0.5 as double) as num, bdd('f=3') as bdd
    union all
    select 1 as group, cast(1.5 as double) as num, bdd('g=2') as bdd
    union all
    select 1 as group, cast(2.0 as double) as num, bdd('g=1') as bdd
    union all
    select 1 as group, cast(0.5 as double) as num, bdd('h=2') as bdd
    union all
    select 1 as group, cast(1.5 as double) as num, bdd('b=3') as bdd
    union all
    select 1 as group, cast(2.0 as double) as num, bdd('c=4') as bdd
    union all
    select 1 as group, cast(0.5 as double) as num, bdd('r=4') as bdd
    union all
    select 1 as group, cast(1.5 as double) as num, bdd('r=3') as bdd
    union all
    select 1 as group, cast(2.0 as double) as num, bdd('h=9') as bdd
    union all
    select 1 as group, cast(0.5 as double) as num, bdd('z=1') as bdd
    union all
    select 1 as group, cast(1.5 as double) as num, bdd('y=3') as bdd
    union all
    select 1 as group, cast(2.0 as double) as num, bdd('x=2') as bdd
    union all
    select 1 as group, cast(0.5 as double) as num, bdd('x=8') as bdd
    union all
    select 1 as group, cast(1.5 as double) as num, bdd('y=7') as bdd
    union all
    select 1 as group, cast(0.5 as double) as num, bdd('g=9') as bdd
    union all
    select 1 as group, cast(1.5 as double) as num, bdd('z=8') as bdd
    union all
    select 1 as group, cast(2.0 as double) as num, bdd('z=5') as bdd
    union all
    select 1 as group, cast(0.5 as double) as num, bdd('r=1') as bdd
    union all
    select 1 as group, cast(1.5 as double) as num, bdd('b=1') as bdd
    union all
    select 1 as group, cast(2.0 as double) as num, bdd('p=3') as bdd
    union all
    select 1 as group, cast(0.5 as double) as num, bdd('p=3') as bdd
    union all
    select 1 as group, cast(1.5 as double) as num, bdd('h=4') as bdd

)

select *
from source_data
