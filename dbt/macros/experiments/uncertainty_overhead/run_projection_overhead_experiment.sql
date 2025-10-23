{%- macro run_projection_overhead_experiment(experiment_name) -%}

    {%- call statement('experiment', fetch_result=False) -%}

    select
        row_num,
        {% if experiment_name == 'large_strings' %}
        string,
        {% elif experiment_name != 'control' %}
        bdd,
        {% endif %}
        {% for i in range(1, 101, 2) %}
        column_{{ i }}
        {% if not loop.last %}
        ,
        {% endif %}
        {% endfor %}
    from experiments.selection_uncertainty_overhead_dataset__{{ experiment_name }}

    {%- endcall -%}

{%- endmacro -%}
