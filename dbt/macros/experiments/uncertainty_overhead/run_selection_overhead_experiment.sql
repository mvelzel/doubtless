{%- macro run_selection_overhead_experiment(experiment_name) -%}

    {%- call statement('experiment', fetch_result=False) -%}

    select *
    from experiments.projection_selection_uncertainty_overhead_dataset__{{ experiment_name }}
    where row_num % 2 = 1

    {%- endcall -%}

{%- endmacro -%}

