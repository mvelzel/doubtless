with dummy_data as (

    {{ generate_bdd_dummy_data(
        groups=1,
        group_variables=5,
        variable_alternatives=5
    ) }}

)

select * from dummy_data
