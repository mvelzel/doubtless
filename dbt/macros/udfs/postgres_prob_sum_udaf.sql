{% macro create_postgres_prob_sum_temptable() %}
    create or replace function prob_sum_reduce_temptable(results_table text, number float8, bdd bdd)
        returns text
        language plpgsql
        volatile
        as $$ begin
            if results_table is null then
                results_table := 'prob_agg_' || uuid_generate_v4();
                execute format(
                    'create temp table %I (sum float8 primary key, sentence bdd) on commit drop',
                    results_table
                );

                execute format('insert into %I (sum, sentence) values (0.0, ''1''::bdd)', results_table);
            end if;

            execute format(
                'insert into %1$I (sum, sentence) '
                '(select sum, agg_or(sentence) as sentence '
                'from (select sum + $1 as sum, sentence & $2 as sentence from %1$I '
                '    union all '
                '    select sum, sentence & (! $2) as sentence from %1$I) '
                'group by sum) '
                'on conflict (sum) do update set sentence = excluded.sentence',
                results_table
            ) using number, bdd;

            return results_table;
        end $$;

    create or replace function prob_sum_final_temptable(results_table text)
        returns text
        language plpgsql
        volatile
        as $$ begin
            if results_table is null then
                results_table := 'prob_agg_' || uuid_generate_v4();
                execute format(
                    'create temp table %I (sum float8 primary key, sentence bdd) on commit drop',
                    results_table
                );

                execute format('insert into %I (sum, sentence) values (0.0, ''1''::bdd)', results_table);
            end if;

            return results_table;
        end $$;
    
    drop aggregate if exists prob_sum (float8, bdd);
    create aggregate if exists prob_sum (float8, bdd)
    (
        sfunc = prob_sum_reduce_temptable,
        stype = text,
        finalfunc = prob_sum_final_temptable
    );
{% endmacro %}

{% macro create_postgres_prob_sum_inmemory() %}
    drop type if exists prob_sum_record cascade;
    create type prob_sum_record as (
        sum float8,
        sentence bdd
    );

    create or replace function prob_sum_reduce_inmemory(prob_sum_record[], float8, bdd)
        returns prob_sum_record[]
        immutable
        language sql
        return
        (
            with unnested_bdds as (
                select
                    record.sum + $2 as sum,
                    record.sentence & $3 as sentence
                from unnest($1) as record
                union all
                select
                    record.sum,
                    record.sentence & (!$3) as sentence
                from unnest($1) as record
            ),
            grouped_bdds as (
                select
                    bdds.sum,
                    agg_or(bdds.sentence) as sentence
                from unnested_bdds bdds
                group by bdds.sum
            )
            select array_agg(row(bdds.sum, bdds.sentence)::prob_sum_record)
            from grouped_bdds bdds
        );

    drop aggregate if exists prob_sum (float8, bdd);
    create aggregate prob_sum (float8, bdd)
    (
        sfunc = prob_sum_reduce_inmemory,
        stype = prob_sum_record[],
        initcond = '{"(0.0, \"1\")"}'
    );
{% endmacro %}
