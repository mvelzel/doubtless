{% macro create_postgres_prob_max_temptable() %}
    select 1;
{% endmacro %}

{% macro create_postgres_prob_max_inmemory() %}
    drop type if exists prob_max_record cascade;
    create type prob_max_record as (
        max float8,
        sentence bdd
    );

    create or replace function prob_max_reduce_inmemory(prob_max_record[], float8, bdd)
        returns prob_max_record[]
        immutable
        language sql
        return
        (
            with unnested_bdds as (
                select
                    $2 as max,
                    $3 & (! agg_or(
                        case when record.max is null
                            then bdd('0')
                            else record.sentence
                        end)) as sentence
                from unnest($1) as record
                where record.max > $2
                or record.max is null
                union all
                select
                    record.max,
                    case when record.max < $2 or record.max is null
                        then record.sentence & (! $3)
                        else record.sentence
                    end as bdd
                from unnest($1) as record
            ),
            grouped_bdds as (
                select
                    bdds.max,
                    agg_or(bdds.sentence) as sentence
                from unnested_bdds bdds
                group by bdds.max
            )
            select array_agg(row(bdds.max, bdds.sentence)::prob_max_record)
            from grouped_bdds bdds
        );

    create or replace function prob_max_final_inmemory(prob_max_record[])
        returns prob_max_record[]
        immutable
        language sql
        return (
            select array_agg(record)
            from unnest($1) as record, experiments.experiments_config as config
            where config['aggregations']['prune-method'] != 'on-finish'
            or not bdd_fast_equiv(record.sentence, bdd('0'))
        );

    drop aggregate if exists prob_max (float8, bdd);
    create aggregate prob_max (float8, bdd)
    (
        sfunc = prob_max_reduce_inmemory,
        stype = prob_max_record[],
        finalfunc = prob_max_final_inmemory,
        initcond = '{"(,\"1\")"}'
    );
{% endmacro %}
