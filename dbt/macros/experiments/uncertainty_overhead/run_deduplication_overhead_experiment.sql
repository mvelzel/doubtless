{%- macro run_deduplication_overhead_experiment(experiment_name) -%}

    {%- call statement('experiment', fetch_result=False) -%}

    select
        row_num,
        {% if experiment_name != 'control' and experiment_name != 'large_strings' and experiment_name != 'medium_strings' %}
        bdd_agg_or(bdd) as bdd,
        {% elif experiment_name == 'large_strings' or experiment_name == 'medium_strings' %}
        string,
        {% endif %}
        {% for i in range(1, 101) %}
        column_{{ i }}
        {% if not loop.last %}
        ,
        {% endif %}
        {% endfor %}
    from experiments.deduplication_uncertainty_overhead_dataset__{{ experiment_name }}
    group by
        row_num,
        {% if experiment_name == 'large_strings' or experiment_name == 'medium_strings' %}
        string,
        {% endif %}
        {% for i in range(1, 101) %}
        column_{{ i }}
        {% if not loop.last %}
        ,
        {% endif %}
        {% endfor %}

    {%- endcall -%}

{%- endmacro -%}
