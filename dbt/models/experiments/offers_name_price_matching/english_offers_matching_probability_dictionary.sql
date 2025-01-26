with offers_with_sentences as (

    select * from {{ ref('int_english_offers_matching_sentence_enriched') }}
    -- If cluster_id is larger than 157201 then the dict index will exceed a hard limit of USHRT_MAX=65535
    -- where cluster_id <= 157201

),

name_variables as (

    select
        concat(
            name_matching_variable_name,
            '=',
            name_matching_variable_alternative,
            ':',
            name_matching_probability
        ) as variable_definition
    from offers_with_sentences
    group by
        name_matching_variable_name,
        name_matching_variable_alternative,
        name_matching_probability
    having name_matching_probability != 1

),

price_variables as (

    select
        concat(
            price_matching_variable_name,
            '=',
            price_matching_variable_alternative,
            ':',
            price_matching_probability
        ) as variable_definition
    from offers_with_sentences
    group by
        price_matching_variable_name,
        price_matching_variable_alternative,
        price_matching_probability
    having price_matching_probability != 1

),

combined_variables as (

    select variable_definition from name_variables
    union all
    select variable_definition from price_variables
    order by variable_definition

),

dictionary_definition as (

    select
        concat_ws(';', collect_list(variable_definition)) as definition,
        count(*) as variable_definition_count
    from combined_variables

),

dictionary as (

    select
        prob_dict(definition) as dictionary,
        variable_definition_count
    from dictionary_definition

)

select * from dictionary
