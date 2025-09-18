{% macro create_postgres_prob_count_temptable() %}
    drop type if exists prob_count_temptable_inter cascade;
    create type prob_count_temptable_inter as (
        results_table text,
        prune_method text
    );

    create or replace function prob_count_reduce_temptable(
        inter prob_count_temptable_inter,
        bdd bdd,
        prune_method text
    )
        returns prob_count_temptable_inter
        language plpgsql
        volatile
        as $$ begin
            if inter is null then
                inter := row('prob_agg_' || uuid_generate_v4(), prune_method)::prob_count_temptable_inter;
                execute format(
                    'create temp table %I (count integer primary key, sentence bdd) on commit drop',
                    inter.results_table
                );

                execute format('insert into %I (count, sentence) values (0, ''1''::bdd)', inter.results_table);
            end if;

            execute format(
                'insert into %1$I (count, sentence) '
                '(select '
                    'count + 1 as count, '
                    'case when $2 = ''each-operation'' '
                        'then prune_and(sentence, $1) '
                        'else sentence & $1 '
                    'end as sentence '
                'from %1$I'
                'union all '
                'select 0 as count, ''0''::bdd as sentence) '
                'on conflict (count) do update set sentence = case when $2 = ''each-operation'' ' 
                    'then excluded.sentence | prune_and(%1$I.sentence, ! $1) '
                    'else excluded.sentence | (%1$I.sentence & (! $1)) '
                'end',
                inter.results_table
            ) using bdd, prune_method;

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

    create or replace function prob_count_final_temptable(inter prob_count_temptable_inter)
        returns text
        language plpgsql
        volatile
        as $$ begin
            if inter is null then
                inter := row('prob_agg_' || uuid_generate_v4(), null)::prob_count_temptable_inter;
                execute format(
                    'create temp table %I (count integer primary key, sentence bdd) on commit drop',
                    inter.results_table
                );

                execute format('insert into %I (count, sentence) values (0, ''1''::bdd)', inter.results_table);
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

    drop aggregate if exists prob_count (bdd);
    drop aggregate if exists prob_count (bdd, text);
    create aggregate prob_count (bdd, text)
    (
        sfunc = prob_count_reduce_temptable,
        stype = prob_count_temptable_inter,
        finalfunc = prob_count_final_temptable
    );
{% endmacro %}

{% macro create_postgres_prob_count_inmemory() %}
    drop type if exists prob_count_record cascade;
    create type prob_count_record as (
        count int,
        sentence bdd
    );

    drop type if exists prob_count_inmemory_inter cascade;
    create type prob_count_inmemory_inter as (
        sentence bdd,
        prune_method text
    );

    create or replace function prob_count_reduce_inmemory(inter prob_count_inmemory_inter[], bdd, text)
        returns prob_count_inmemory_inter[]
        immutable
        language sql
        return
        (
            with unnested_bdds as (
                select
                    row(case when $3 = 'each-operation'
                        then prune_and($1[1].sentence, ! $2)
                        else $1[1].sentence & (! $2)
                    end, $3)::prob_count_inmemory_inter as inter
                union all
                select
                    row(case when next_bdd is null
                        then
                            case when $3 = 'each-operation'
                                then prune_and(cur_bdd, $2)
                                else cur_bdd & $2
                            end
                        else
                            case when cur_bdd is null
                                then
                                    case when $3 = 'each-operation'
                                        then prune_and(next_bdd, ! $2)
                                        else next_bdd & (! $2)
                                    end
                                else
                                    case when $3 = 'each-operation'
                                        then prune_and(cur_bdd, $2) | prune_and(next_bdd, ! $2)
                                        else (cur_bdd & $2) | (next_bdd & (! $2))
                                    end
                                end
                    end, $3)::prob_count_inmemory_inter as inter
                from unnest($1, $1[2:]) as x(cur_bdd,_,next_bdd,_)
            ),
            optionally_filtered_bdds as (
                select case $3
                    when 'each-step' then
                        case when bdd_fast_equiv((inter).sentence, '0'::bdd)
                            then row(null, $3)::prob_count_inmemory_inter
                            else inter
                        end
                    when 'each-operation' then
                        case when bdd_equal((inter).sentence, '0'::bdd)
                            then row(null, $3)::prob_count_inmemory_inter
                            else inter
                        end
                    else inter
                end
                from unnested_bdds
            )
            select array_agg(inter)
            from optionally_filtered_bdds
        );

    create or replace function prob_count_final_inmemory(prob_count_inmemory_inter[])
        returns prob_count_record[]
        immutable
        language sql
        return
        (
            select array_agg(row(cp1 - 1, sentence)::prob_count_record)
            from unnest($1) with ordinality as x(sentence, _, cp1)
            where case when $1[1].prune_method = 'on-finish'
                then not bdd_fast_equiv(sentence, '0'::bdd)
                else true
            end
            and sentence is not null
        );

    drop aggregate if exists prob_count (bdd);
    drop aggregate if exists prob_count (bdd, text);
    create aggregate prob_count (bdd, text)
    (
        sfunc = prob_count_reduce_inmemory,
        stype = prob_count_inmemory_inter[],
        finalfunc = prob_count_final_inmemory,
        initcond = '{"(\"1\", \"none\")"}'
    );
{% endmacro %}
