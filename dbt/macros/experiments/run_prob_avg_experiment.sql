{%- macro run_prob_avg_experiment(experiment_name) -%}

    {% set sql %}
    with grouped as (
        {% if target.name == 'spark' or target.name == 'databricks' -%}
        select prob_avg(cast(number as double), sentence, '{{ var("prune_method") }}') as map
        {% elif target.name == 'postgres' -%}
        select prob_avg(cast(number as double precision), sentence, '{{ var("prune_method") }}') as map
        {% endif -%}
        from experiments.probabilistic_avg_dataset
        where experiment_name = '{{ experiment_name }}'
        group by group_index
    )

    {% if target.name == 'spark' or target.name == 'databricks' %}
        select record.avg as avg, record.bdd as sentence
        from grouped lateral view explode(map) explodeVal as record;
    {% elif target.name == 'postgres' %}
        select res.avg from grouped
        left join lateral (
            select * from consume_prob_agg_results(grouped.map) as res(avg float8,sentence bdd)
        ) res on true;
    {% endif %}
    {% endset %}

    {% do run_query(sql) %}

{%- endmacro -%}
