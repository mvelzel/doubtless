{% macro create_postgres_prob_avg_temptable() %}
    select 1;
{% endmacro %}

{% macro create_postgres_prob_avg_inmemory() %}
    drop type if exists prob_avg_partial_record cascade;
    create type prob_avg_partial_record as (
        count int,
        sum float8,
        sentence bdd
    );

    drop type if exists prob_avg_record cascade;
    create type prob_avg_record as (
        avg float8,
        sentence bdd
    );

    create or replace function prob_avg_reduce_inmemory(prob_avg_partial_record[], float8, bdd)
        returns prob_avg_partial_record[]
        immutable
        language sql
        return
        (
            with unnested_bdds as (
                select
                    record.count + 1 as count,
                    record.sum + $2 as sum,
                    record.sentence & $3 as sentence
                from unnest($1) as record
                union all
                select
                    record.count,
                    record.sum,
                    record.sentence & (!$3) as sentence
                from unnest($1) as record
            ),
            grouped_bdds as (
                select
                    bdds.count,
                    bdds.sum,
                    agg_or(bdds.sentence) as sentence
                from unnested_bdds bdds
                group by bdds.count, bdds.sum
            )
            select array_agg(row(bdds.count, bdds.sum, bdds.sentence)::prob_avg_partial_record)
            from grouped_bdds bdds
        );

    create or replace function prob_avg_final_inmemory(prob_avg_partial_record[])
        returns prob_avg_record[]
        immutable
        language sql
        return
        (
            select array_agg(row(case when record.count = 0
                then null
                else record.sum / record.count
            end, record.sentence)::prob_avg_record)
            from unnest($1) as record
        );

    drop aggregate if exists prob_avg (float8, bdd);
    create aggregate prob_avg (float8, bdd)
    (
        sfunc = prob_avg_reduce_inmemory,
        stype = prob_avg_partial_record[],
        finalfunc = prob_avg_final_inmemory,
        initcond = '{"(0, 0.0, \"1\")"}'
    );
{% endmacro %}
