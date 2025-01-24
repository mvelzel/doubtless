
-- Use the `ref` function to select from other models

select *
from {{ ref('my_first_dbt_model') }}
where bdd_to_string(bdd) = 'BDD(x=1)'
