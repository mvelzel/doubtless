{%- macro run_prob_sum_experiment(experiment_name) -%}

    {% set sql %}
    with grouped as (
        {% if target.name == 'spark' %}
        select prob_sum(cast(number as double), sentence) as map
        {% else %}
        select prob_sum(number, sentence) as map
        {% endif %}
        from experiments.probabilistic_sum_dataset
        where experiment_name = '{{ experiment_name }}'
        group by group_index
    )

    {% if target.name == 'spark' %}
        select record.sum as sum, record.bdd as sentence
        from grouped lateral view explode(map) explodeVal as record;
    {% elif target.name == 'postgres' %}
        select res.sum from grouped
        left join lateral (
            select * from {{ consume_prob_agg_results('grouped.map') }} as res(sum float8,sentence bdd)
        ) res on true;
    {% endif %}
    {% endset %}

    {% do run_query(sql) %}

{%- endmacro -%}
