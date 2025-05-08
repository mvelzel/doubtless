{%- macro run_prob_count_experiment(experiment_name) -%}

    {% set sql %}
    with grouped as (
        select prob_count(sentence) as map
        from experiments.probabilistic_count_dataset
        where experiment_name = '{{ experiment_name }}'
        group by "group"
    )

    {% if target.name == 'spark' %}
        select key as count, value as sentence
        from grouped lateral view explode(map) explodeVal as key, value
        order by count asc;
    {% elif target.name == 'postgres' %}
        select x.cp1 - 1 as count, x.bdd as sentence from grouped
        left join lateral unnest(map) with ordinality as x(bdd, cp1) on true;
    {% endif %}
    {% endset %}

    {% do run_query(sql) %}

{%- endmacro -%}
