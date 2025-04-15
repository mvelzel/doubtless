{%- macro run_prob_sum_experiment(experiment_name) -%}

    {% set sql %}
    with grouped as (
        select prob_sum(number, sentence) as map
        from experiments.probabilistic_sum_dataset
        where experiment_name = '{{ experiment_name }}'
        group by group
    )

    select key as sum, value as sentence
    from grouped lateral view explode(map) explodeVal as key, value
    --where not bdd_equiv(value, bdd('0'))
    order by sum asc;
    {% endset %}

    {% do run_query(sql) %}

{%- endmacro -%}
