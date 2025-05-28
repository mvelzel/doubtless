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

    create or replace function consume_prob_agg_results_temptable(results_table text)
        returns setof record
        language plpgsql
        volatile
        as $$ begin
            return query execute format('select * from %I', results_table);

            execute format('drop table %I', results_table);
            return;
        end $$;

    drop aggregate prob_count (bdd);
    create aggregate prob_count (bdd)
    (
        sfunc = prob_count_reduce_temptable,
        stype = text,
        finalfunc = prob_count_final
    );
{% endmacro %}

{% macro create_postgres_prob_count_inmemory() %}
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

    create or replace function consume_prob_agg_results_inmemory(bdd[])
        returns setof record
        language plpgsql
        immutable
        as $$ begin
            return query select
                (ord - 1)::integer as count,
                bdd as sentence
            from unnest($1) with ordinality as x(bdd, ord);
        end $$;

    drop aggregate prob_count (bdd);
    create aggregate prob_count (bdd)
    (
        sfunc = prob_count_reduce_inmemory,
        stype = bdd[],
        initcond = '{"1"}'
    );
{% endmacro %}

{% macro consume_prob_agg_results(field) %}

    {% if target.name == 'spark' %}
        select 1;
    {%- elif target.name == 'postgres' -%}
        consume_prob_agg_results_inmemory({{ field }})
    {% endif %}

{% endmacro %}

{% macro create_prob_count_udaf() %}

    {% if target.name == 'spark' %}
        create or replace function prob_count as 'com.doubtless.spark.hive.HiveProbCountGenericUDAF';
    {%- elif target.name == 'postgres' -%}
        {{ create_postgres_prob_count_inmemory() }}
    {% endif %}

{% endmacro %}

{% macro create_prob_sum_udaf() %}

    {% if target.name == 'spark' %}
        create or replace function prob_sum as 'com.doubtless.spark.hive.HiveProbSumGenericUDAF';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}

{% macro create_prob_min_udaf() %}

    {% if target.name == 'spark' %}
        create or replace function prob_min as 'com.doubtless.spark.hive.HiveProbMinGenericUDAF';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}

{% macro create_prob_max_udaf() %}

    {% if target.name == 'spark' %}
        create or replace function prob_max as 'com.doubtless.spark.hive.HiveProbMaxGenericUDAF';
    {% else %}
        select 1;
    {% endif %}

{% endmacro %}
