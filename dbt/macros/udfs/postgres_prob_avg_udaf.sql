{% macro create_postgres_prob_avg_temptable() %}
    drop type if exists prob_avg_temptable_inter cascade;
    create type prob_avg_temptable_inter as (
        results_table text,
        prune_method text
    );

    create or replace function prob_avg_reduce_temptable(
        inter prob_avg_temptable_inter,
        number float8,
        bdd bdd,
        prune_method text
    )
        returns prob_avg_temptable_inter
        language plpgsql
        volatile
        as $$ begin
            if inter is null then
                inter := row('prob_agg_' || uuid_generate_v4(), prune_method)::prob_avg_temptable_inter;
                execute format(
                    'create temp table %I (count int, sum float8, sentence bdd, primary key (count, sum)) on commit drop',
                    inter.results_table
                );

                execute format('insert into %I (count, sum, sentence) values (0, 0.0, ''1''::bdd)', inter.results_table);
            end if;

            execute format(
                'insert into %1$I (count, sum, sentence) '
                '(select count, sum, agg_or(sentence) as sentence '
                'from ( '
                    'select '
                        'count + 1 as count, '
                        'sum + $1 as sum, '
                        'case when $3 = ''each-operation'' '
                            'then prune_and(sentence, $2) '
                            'else sentence & $2 '
                        'end as sentence from %1$I '
                    'union all '
                    'select '
                        'count, '
                        'sum, '
                        'case when $3 = ''each-operation'' '
                            'then prune_and(sentence, ! $2) '
                            'else sentence & (! $2) '
                        'end as sentence from %1$I '
                ') '
                'group by count, sum) '
                'on conflict (count, sum) do update set sentence = excluded.sentence',
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

    create or replace function prob_avg_final_temptable(inter prob_avg_temptable_inter)
        returns text
        language plpgsql
        volatile
        as $$
        declare results_table text;
        begin
            results_table := 'prob_agg_' || uuid_generate_v4();
            execute format(
                'create temp table %I (avg float8, sentence bdd) on commit drop',
                results_table
            );

            if inter is null then
                execute format('insert into %I (avg, sentence) values (null, ''1''::bdd)', results_table);
            else
                execute format(
                    'insert into %1$I (avg, sentence) '
                    'select '
                        'trunc(cast(sum / nullif(count, 0) as numeric), 10) as avg, '
                        'agg_or(sentence) as sentence '
                    'from %2$I '
                    'group by trunc(cast(sum / nullif(count, 0) as numeric), 10)',
                    results_table,
                    inter.results_table
                );

                if inter.prune_method = 'on-finish' then
                    execute format(
                        'delete from %I '
                        'where bdd_fast_equiv(sentence, ''0''::bdd)',
                        results_table
                    );
                end if;

                execute format(
                    'drop table %I',
                    inter.results_table
                );
            end if;

            return results_table;
        end $$;
    
    drop aggregate if exists prob_avg (float8, bdd);
    drop aggregate if exists prob_avg (float8, bdd, text);
    create aggregate prob_avg (float8, bdd, text)
    (
        sfunc = prob_avg_reduce_temptable,
        stype = prob_avg_temptable_inter,
        finalfunc = prob_avg_final_temptable
    );
{% endmacro %}

{% macro create_postgres_prob_avg_inmemory() %}
    drop type if exists prob_avg_partial_record cascade;
    create type prob_avg_partial_record as (
        count int,
        sum float8,
        sentence bdd,
        prune_method text
    );

    drop type if exists prob_avg_record cascade;
    create type prob_avg_record as (
        avg float8,
        sentence bdd
    );

    create or replace function prob_avg_reduce_inmemory(prob_avg_partial_record[], float8, bdd, text)
        returns prob_avg_partial_record[]
        immutable
        as
        $$
            with unnested_bdds as (
                select
                    record.count + 1 as count,
                    record.sum + $2 as sum,
                    case when $4 = 'each-operation'
                        then prune_and(record.sentence, $3)
                        else record.sentence & $3
                    end as sentence
                from unnest($1) as record
                union all
                select
                    record.count,
                    record.sum,
                    case when $4 = 'each-operation'
                        then prune_and(record.sentence, !$3)
                        else record.sentence & (!$3)
                    end as sentence
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
            select array_agg(row(bdds.count, bdds.sum, bdds.sentence, $4)::prob_avg_partial_record)
            from grouped_bdds bdds
            where case $4
                when 'each-step' then not bdd_fast_equiv(bdds.sentence, '0'::bdd)
                when 'each-operation' then not bdd_equal(bdds.sentence, '0'::bdd)
                else true
            end
        $$ language sql;

    create or replace function prob_avg_final_inmemory(prob_avg_partial_record[])
        returns prob_avg_record[]
        immutable
        as
        $$
            select array_agg(row(record.sum, record.sentence)::prob_avg_record)
            from (
                select
                    trunc(cast(record.sum / nullif(record.count, 0) as numeric), 10) as sum,
                    agg_or(record.sentence) sentence
                from unnest($1) as record
                group by trunc(cast(record.sum / nullif(record.count, 0) as numeric), 10)
            ) as record
            where case when $1[1].prune_method = 'on-finish'
                then not bdd_fast_equiv(sentence, '0'::bdd)
                else true
            end
        $$ language sql;

    drop aggregate if exists prob_avg (float8, bdd);
    drop aggregate if exists prob_avg (float8, bdd, text);
    create aggregate prob_avg (float8, bdd, text)
    (
        sfunc = prob_avg_reduce_inmemory,
        stype = prob_avg_partial_record[],
        finalfunc = prob_avg_final_inmemory,
        initcond = '{"(0, 0.0, \"1\", \"none\")"}'
    );
{% endmacro %}
