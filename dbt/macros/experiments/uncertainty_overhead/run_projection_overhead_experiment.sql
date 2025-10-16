{%- macro run_projection_overhead_experiment(experiment_name) -%}

    {% set sql %}
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
    from experiments.projection_selection_uncertainty_overhead_dataset__{{ experiment_name }}
    {% endset %}

    {% do run_query(sql) %}

{%- endmacro -%}
