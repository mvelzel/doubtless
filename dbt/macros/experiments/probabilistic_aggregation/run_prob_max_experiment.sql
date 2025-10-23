{%- macro run_prob_max_experiment(experiment_name) -%}

    {%- call statement('experiment', fetch_result=False) -%}

    with grouped as (
        {% if target.name == 'spark' or target.name == 'databricks' -%}
        select prob_max(cast(number as double), sentence, '{{ var("prune_method") }}') as map
        {% elif target.name == 'postgres' -%}
        select prob_max(cast(number as double precision), sentence, '{{ var("prune_method") }}') as map
        {% endif -%}
        from {{ ref('probabilistic_max_dataset') }}
        where experiment_name = '{{ experiment_name }}'
        group by group_index
    )

    {% if target.name == 'spark' or target.name == 'databricks' %}
        select record.max as max, record.bdd as sentence
        from grouped lateral view explode(map) explodeVal as record;
    {% elif target.name == 'postgres' %}
        select res.max from grouped
        left join lateral (
            select * from consume_prob_agg_results(grouped.map) as res(max double precision,sentence bdd)
        ) res on true;
    {% endif %}

    {%- endcall -%}

{%- endmacro -%}
