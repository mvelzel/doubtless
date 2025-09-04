{%- macro run_prob_min_experiment(experiment_name) -%}

    {% set sql %}
    with grouped as (
        {% if target.name == 'spark' or target.name == 'databricks' %}
        select prob_min(cast(number as double), sentence, '{{ var("prune_method") }}') as map
        {% else %}
        select prob_min(number, sentence) as map
        {% endif %}
        from experiments.probabilistic_min_dataset
        where experiment_name = '{{ experiment_name }}'
        group by group_index
    )

    {% if target.name == 'spark' or target.name == 'databricks' %}
        select record.min as min, record.bdd as sentence
        from grouped lateral view explode(map) explodeVal as record;
    {% elif target.name == 'postgres' %}
        select res.min from grouped
        left join lateral (
            select * from {{ consume_prob_agg_results('grouped.map') }} as res(min integer,sentence bdd)
        ) res on true;
    {% endif %}
    {% endset %}

    {% do run_query(sql) %}

{%- endmacro -%}
