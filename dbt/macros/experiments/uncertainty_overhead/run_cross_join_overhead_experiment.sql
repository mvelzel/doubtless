{%- macro run_cross_join_overhead_experiment(experiment_name) -%}

    {% set sql %}
    select
        {% if experiment_name != 'control' and experiment_name != 'large_strings' %}
        bdd_and(l.bdd, r.bdd) as bdd,
        {% endif %}
        *
    from experiments.join_uncertainty_overhead_dataset__{{ experiment_name }}__left as l
    cross join experiments.join_uncertainty_overhead_dataset__{{ experiment_name }}__right as r
    {% endset %}

    {% do run_query(sql) %}

{%- endmacro -%}
