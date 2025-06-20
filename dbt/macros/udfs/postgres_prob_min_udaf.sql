{% macro create_postgres_prob_min_temptable() %}
    select 1;
{% endmacro %}

{% macro create_postgres_prob_min_inmemory() %}
    drop type if exists prob_min_record cascade;
    create type prob_min_record as (
        min float8,
        sentence bdd
    );

    create or replace function prob_min_reduce_inmemory(prob_min_record[], float8, bdd)
        returns prob_min_record[]
        immutable
        language sql
        return
        (
            with unnested_bdds as (
                select
                    $2 as min,
                    $3 & (! agg_or(
                        case when record.min is null
                            then bdd('0')
                            else record.sentence
                        end)) as sentence
                from unnest($1) as record
                where record.min < $2
                or record.min is null
                union all
                select
                    record.min,
                    case when record.min > $2 or record.min is null
                        then record.sentence & (! $3)
                        else record.sentence
                    end as bdd
                from unnest($1) as record
            ),
            grouped_bdds as (
                select
                    bdds.min,
                    agg_or(bdds.sentence) as sentence
                from unnested_bdds bdds
                group by bdds.min
            )
            select array_agg(row(bdds.min, bdds.sentence)::prob_min_record)
            from grouped_bdds bdds
        );

    create or replace function prob_min_final_inmemory(prob_min_record[])
        returns prob_min_record[]
        immutable
        language sql
        return (
            select array_agg(record)
            from unnest($1) as record, experiments.experiments_config as config
            where config['aggregations']['prune-method'] != 'on-finish'
            or not bdd_fast_equiv(record.sentence, bdd('0'))
        );

    drop aggregate if exists prob_min (float8, bdd);
    create aggregate prob_min (float8, bdd)
    (
        sfunc = prob_min_reduce_inmemory,
        stype = prob_min_record[],
        finalfunc = prob_min_final_inmemory,
        initcond = '{"(,\"1\")"}'
    );
{% endmacro %}
