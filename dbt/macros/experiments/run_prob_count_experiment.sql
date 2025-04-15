{%- macro run_prob_count_experiment(experiment_name) -%}

    {% set sql %}
    with grouped as (
        select prob_count(sentence) as map
        from experiments.probabilistic_count_dataset
        where experiment_name = '{{ experiment_name }}'
        group by group
    )

    select key as count, value as sentence
    from grouped lateral view explode(map) explodeVal as key, value
    order by count asc;
    {% endset %}

    {% do run_query(sql) %}

{%- endmacro -%}
