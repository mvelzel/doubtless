{%- macro run_inner_join_overhead_experiment(experiment_name) -%}

    {% set sql %}
    select
        {% if experiment_name != 'control' and experiment_name != 'large_strings' and experiment_name != 'medium_strings' %}
        bdd_and(l.bdd, r.bdd) as bdd,
        {% endif %}
        *
    from experiments.join_uncertainty_overhead_dataset__{{ experiment_name }}__left as l
    inner join experiments.join_uncertainty_overhead_dataset__{{ experiment_name }}__right as r
    on l.row_num = r.row_num
    {% endset %}

    {% do run_query(sql) %}

{%- endmacro -%}
