{% macro create_postgres_prob_sum_temptable() %}
    drop type if exists prob_sum_temptable_inter cascade;
    create type prob_sum_temptable_inter as (
        results_table text,
        prune_method text
    );

    create or replace function prob_sum_reduce_temptable(
        inter prob_sum_temptable_inter,
        number float8,
        bdd bdd,
        prune_method text
    )
        returns prob_sum_temptable_inter
        language plpgsql
        volatile
        as $$ begin
            if inter is null then
                inter := row('prob_agg_' || uuid_generate_v4(), prune_method)::prob_sum_temptable_inter;
                execute format(
                    'create temp table %I (sum float8 primary key, sentence bdd) on commit drop',
                    inter.results_table
                );

                execute format('insert into %I (sum, sentence) values (0.0, ''1''::bdd)', inter.results_table);
            end if;

            execute format(
                'insert into %1$I (sum, sentence) '
                '(select sum, agg_or(sentence) as sentence '
                'from ( '
                    'select '
                        'sum + $1 as sum, '
                        'case when $3 = ''each-operation'' '
                            'then prune_and(sentence, $2) '
                            'else sentence & $2 '
                        'end as sentence from %1$I '
                    'union all '
                    'select '
                        'sum, '
                        'case when $3 = ''each-operation'' '
                            'then prune_and(sentence, ! $2) '
                            'else sentence & (! $2) '
                        'end as sentence from %1$I '
                ') '
                'group by sum) '
                'on conflict (sum) do update set sentence = excluded.sentence',
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

    create or replace function prob_sum_final_temptable(inter prob_sum_temptable_inter)
        returns text
        language plpgsql
        volatile
        as $$ begin
            if inter is null then
                inter := row('prob_agg_' || uuid_generate_v4(), null)::prob_sum_temptable_inter;
                execute format(
                    'create temp table %I (sum float8 primary key, sentence bdd) on commit drop',
                    inter.results_table
                );

                execute format('insert into %I (sum, sentence) values (0.0, ''1''::bdd)', inter.results_table);
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
    
    drop aggregate if exists prob_sum (float8, bdd);
    drop aggregate if exists prob_sum (float8, bdd, text);
    create aggregate prob_sum (float8, bdd, text)
    (
        sfunc = prob_sum_reduce_temptable,
        stype = prob_sum_temptable_inter,
        finalfunc = prob_sum_final_temptable
    );
{% endmacro %}

{% macro create_postgres_prob_sum_inmemory() %}
    drop type if exists prob_sum_record cascade;
    create type prob_sum_record as (
        sum float8,
        sentence bdd
    );

    drop type if exists prob_sum_inmemory_inter cascade;
    create type prob_sum_inmemory_inter as (
        sum float8,
        sentence bdd,
        prune_method text
    );

    create or replace function prob_sum_reduce_inmemory(prob_sum_inmemory_inter[], float8, bdd, text)
        returns prob_sum_inmemory_inter[]
        immutable
        language sql
        return
        (
            with unnested_bdds as (
                select
                    record.sum + $2 as sum,
                    case when $4 = 'each-operation'
                        then prune_and(record.sentence, $3)
                        else record.sentence & $3
                    end as sentence
                from unnest($1) as record
                union all
                select
                    record.sum,
                    case when $4 = 'each-operation'
                        then prune_and(record.sentence, !$3)
                        else record.sentence & (!$3)
                    end as sentence
                from unnest($1) as record
            ),
            grouped_bdds as (
                select
                    bdds.sum,
                    agg_or(bdds.sentence) as sentence
                from unnested_bdds bdds
                group by bdds.sum
            )
            select array_agg(row(bdds.sum, bdds.sentence, $4)::prob_sum_inmemory_inter)
            from grouped_bdds bdds
            where case $4
                when 'each-step' then not bdd_fast_equiv(bdds.sentence, '0'::bdd)
                when 'each-operation' then not bdd_equal(bdds.sentence, '0'::bdd)
                else true
            end
        );

    create or replace function prob_sum_final_inmemory(prob_sum_inmemory_inter[])
        returns prob_sum_record[]
        immutable
        language sql
        return (
            select array_agg(row(record.sum, record.sentence)::prob_sum_record)
            from unnest($1) as record
            where case when $1[1].prune_method = 'on-finish'
                then not bdd_fast_equiv(sentence, '0'::bdd)
                else true
            end
        );

    drop aggregate if exists prob_sum (float8, bdd);
    drop aggregate if exists prob_sum (float8, bdd, text);
    create aggregate prob_sum (float8, bdd, text)
    (
        sfunc = prob_sum_reduce_inmemory,
        stype = prob_sum_inmemory_inter[],
        finalfunc = prob_sum_final_inmemory,
        initcond = '{"(0.0, \"1\", \"none\")"}'
    );
{% endmacro %}
