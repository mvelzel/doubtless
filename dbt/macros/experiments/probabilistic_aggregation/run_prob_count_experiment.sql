{%- macro run_prob_count_experiment(experiment_name) -%}

    {%- call statement('experiment', fetch_result=False) -%}

    with grouped as (
        select prob_count(sentence, '{{ var("prune_method") }}') as map
        from {{ ref('probabilistic_count_dataset') }}
        where experiment_name = '{{ experiment_name }}'
        group by group_index
    )

    {% if target.name == 'spark' or target.name == 'databricks' %}
        select key as count
        from grouped lateral view posexplode(map) explodeVal as key, value
        where value is not null;
    {% elif target.name == 'postgres' %}
        select res.count from grouped
        left join lateral (
            select * from consume_prob_agg_results(grouped.map) as res(count integer,sentence bdd)
        ) res on true;
    {% endif %}

    {%- endcall -%}

{%- endmacro -%}
