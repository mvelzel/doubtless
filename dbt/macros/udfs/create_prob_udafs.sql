{% macro create_prob_count_udaf() %}

    {% if target.name == 'spark' %}
        create or replace function prob_count as 'com.doubtless.spark.hive.HiveProbCountUDAF';
    {%- elif target.name == 'postgres' -%}
        create or replace function prob_count_reduce(bdd[], bdd)
            returns bdd[]
            immutable
            language sql
            return
            (
                with unnested_bdds as (
                    select
                        $1[1] & (! $2) as bdd
                    union all
                    select
                        case when
                            next_bdd is null then cur_bdd & $2
                            else (cur_bdd & $2) | (next_bdd & (! $2))
                        end as bdd
                    from unnest($1, $1[2:]) as x(cur_bdd,next_bdd)
                )
                select array_agg(bdd)
                from unnested_bdds
            );

        drop aggregate prob_count (bdd);
        create aggregate prob_count (bdd)
        (
            sfunc = prob_count_reduce,
            stype = bdd[],
            initcond = '{"1"}'
        );
    {% endif %}

{% endmacro %}

{% macro create_prob_sum_udaf() %}

    {% if target.name == 'spark' %}
        create or replace function prob_sum as 'com.doubtless.spark.hive.HiveProbSumUDAF';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}
