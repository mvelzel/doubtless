{% macro create_postgres_prob_count_temptable() %}
    create or replace function prob_count_reduce_temptable(results_table text, bdd bdd)
        returns text
        language plpgsql
        volatile
        as $$ begin
            if results_table is null then
                results_table := 'prob_agg_' || uuid_generate_v4();
                execute format(
                    'create temp table %I (count integer primary key, sentence bdd) on commit drop',
                    results_table
                );

                execute format('insert into %I (count, sentence) values (0, ''1''::bdd)', results_table);
            end if;

            execute format(
                'insert into %1$I (count, sentence) '
                '(select count + 1 as count, sentence & $1 as sentence from %1$I'
                'union all '
                'select 0 as count, ''0''::bdd as sentence) '
                'on conflict (count) do update set sentence = excluded.sentence | (%1$I.sentence & (! $1))',
                results_table
            ) using bdd;

            return results_table;
        end $$;

    create or replace function prob_count_final_temptable(results_table text)
        returns text
        language plpgsql
        volatile
        as $$ begin
            if results_table is null then
                results_table := 'prob_agg_' || uuid_generate_v4();
                execute format(
                    'create temp table %I (count integer primary key, sentence bdd) on commit drop',
                    results_table
                );

                execute format('insert into %I (count, sentence) values (0, ''1''::bdd)', results_table);
            end if;

            return results_table;
        end $$;

    drop aggregate if exists prob_count (bdd);
    create aggregate prob_count (bdd)
    (
        sfunc = prob_count_reduce_temptable,
        stype = text,
        finalfunc = prob_count_final_temptable
    );
{% endmacro %}

{% macro create_postgres_prob_count_inmemory() %}
    drop type if exists prob_count_record cascade;
    create type prob_count_record as (
        count int,
        sentence bdd
    );

    create or replace function prob_count_reduce_inmemory(bdd[], bdd)
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

    create or replace function prob_count_final_inmemory(bdd[])
        returns prob_count_record[]
        immutable
        language sql
        return
        (
            select array_agg(row(x.cp1 - 1, x.bdd)::prob_count_record)
            from unnest($1) with ordinality as x(bdd, cp1)
        );

    drop aggregate if exists prob_count (bdd);
    create aggregate prob_count (bdd)
    (
        sfunc = prob_count_reduce_inmemory,
        stype = bdd[],
        finalfunc = prob_count_final_inmemory,
        initcond = '{"1"}'
    );
{% endmacro %}
