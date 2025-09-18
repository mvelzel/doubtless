{% macro create_postgres_prob_max_temptable() %}
    drop type if exists prob_max_temptable_inter cascade;
    create type prob_max_temptable_inter as (
        results_table text,
        prune_method text
    );

    create or replace function prob_max_reduce_temptable(
        inter prob_max_temptable_inter,
        number float8,
        bdd bdd,
        prune_method text
    )
        returns prob_max_temptable_inter
        language plpgsql
        volatile
        as $$ begin
            if inter is null then
                inter := row('prob_agg_' || uuid_generate_v4(), prune_method)::prob_max_temptable_inter;
                execute format(
                    'create temp table %I (max float8 primary key, sentence bdd) on commit drop',
                    inter.results_table
                );

                execute format('insert into %I (max, sentence) values (''NaN'', ''1''::bdd)', inter.results_table);
            end if;

            execute format(
                'insert into %1$I (max, sentence) '
                '(select max, agg_or(sentence) as sentence '
                'from ( '
                    'select '
                        '$1 as max, '
                        'case when $3 = ''each-operation'' '
                            'then prune_and($2, ! agg_or( '
                                'case when record.max = ''NaN'' '
                                    'then bdd(''0'') '
                                    'else record.sentence '
                                'end)) '
                            'else $2 & (! agg_or( '
                                'case when record.max = ''NaN'' '
                                    'then bdd(''0'') '
                                    'else record.sentence '
                                'end)) '
                        'end as sentence '
                    'from %1$I as record '
                    'where record.max > $1 '
                    'or record.max = ''NaN'' '
                    'union all '
                    'select '
                        'record.max, '
                        'case when record.max < $1 or record.max = ''NaN'' '
                            'then case when $3 = ''each-operation'' '
                                'then prune_and(record.sentence, ! $2) '
                                'else record.sentence & (! $2) '
                            'end '
                            'else record.sentence '
                        'end as bdd '
                    'from %1$I as record '
                ') '
                'group by max) '
                'on conflict (max) do update set sentence = excluded.sentence',
                inter.results_table
            ) using number, bdd, prune_method;

            if prune_method = 'each-step' then
                execute format(
                    'delete from %1$I '
                    'where bdd_fast_equiv(sentence, ''0''::bdd)',
                    inter.results_table
                );
            elsif prune_method = 'each-operation' then
                execute format(
                    'delete from %1$I '
                    'where bdd_equal(sentence, ''0''::bdd)',
                    inter.results_table
                );
            end if;

            return inter;
        end $$;

    create or replace function prob_max_final_temptable(inter prob_max_temptable_inter)
        returns text
        language plpgsql
        volatile
        as $$ begin
            if inter is null then
                inter := row('prob_agg_' || uuid_generate_v4(), null)::prob_sum_temptable_inter;
                execute format(
                    'create temp table %I (max float8 primary key, sentence bdd) on commit drop',
                    inter.results_table
                );

                execute format('insert into %I (max, sentence) values (''NaN'', ''1''::bdd)', inter.results_table);
            end if;

            if inter.prune_method = 'on-finish' then
                execute format(
                    'delete from %I '
                    'where bdd_fast_equiv(sentence, ''0''::bdd)',
                    inter.results_table
                );
            end if;

            return inter.results_table;
        end $$;

    drop aggregate if exists prob_max (float8, bdd);
    drop aggregate if exists prob_max (float8, bdd, text);
    create aggregate prob_max (float8, bdd, text)
    (
        sfunc = prob_max_reduce_temptable,
        stype = prob_max_temptable_inter,
        finalfunc = prob_max_final_temptable
    );
{% endmacro %}

{% macro create_postgres_prob_max_inmemory() %}
    drop type if exists prob_max_record cascade;
    create type prob_max_record as (
        max float8,
        sentence bdd
    );

    drop type if exists prob_max_inmemory_inter cascade;
    create type prob_max_inmemory_inter as (
        max float8,
        sentence bdd,
        prune_method text
    );

    create or replace function prob_max_reduce_inmemory(prob_max_inmemory_inter[], float8, bdd, text)
        returns prob_max_inmemory_inter[]
        immutable
        language sql
        return
        (
            with unnested_bdds as (
                select
                    $2 as max,
                    case when $4 = 'each-operation'
                        then prune_and($3, ! agg_or(
                            case when record.max is null
                                then bdd('0')
                                else record.sentence
                            end))
                        else $3 & (! agg_or(
                            case when record.max is null
                                then bdd('0')
                                else record.sentence
                            end))
                    end as sentence
                from unnest($1) as record
                where record.max > $2
                or record.max is null
                union all
                select
                    record.max,
                    case when record.max < $2 or record.max is null
                        then case when $4 = 'each-operation'
                            then prune_and(record.sentence, ! $3)
                            else record.sentence & (! $3)
                        end
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
            select array_agg(row(bdds.max, bdds.sentence, $4)::prob_max_inmemory_inter)
            from grouped_bdds bdds
            where case $4
                when 'each-step' then not bdd_fast_equiv(bdds.sentence, '0'::bdd)
                when 'each-operation' then not bdd_equal(bdds.sentence, '0'::bdd)
                else true
            end
        );

    create or replace function prob_max_final_inmemory(prob_max_inmemory_inter[])
        returns prob_max_record[]
        immutable
        language sql
        return (
            select array_agg(row(record.max, record.sentence)::prob_max_record)
            from unnest($1) as record
            where case when $1[1].prune_method = 'on-finish'
                then not bdd_fast_equiv(sentence, '0'::bdd)
                else true
            end
        );

    drop aggregate if exists prob_max (float8, bdd);
    drop aggregate if exists prob_max (float8, bdd, text);
    create aggregate prob_max (float8, bdd, text)
    (
        sfunc = prob_max_reduce_inmemory,
        stype = prob_max_inmemory_inter[],
        finalfunc = prob_max_final_inmemory,
        initcond = '{"(,\"1\", \"none\")"}'
    );
{% endmacro %}
