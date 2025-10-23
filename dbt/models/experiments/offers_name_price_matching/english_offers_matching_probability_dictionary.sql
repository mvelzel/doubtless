with offers_with_sentences as (

    select
        name_matching_probability,
        name_matching_variable_name,
        name_matching_variable_alternative,
        price_matching_probability,
        price_matching_variable_name,
        price_matching_variable_alternative
    from {{ ref('int_english_offers_matching_sentence_enriched') }}

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
        {{ string_agg('variable_definition', ';') }} as definition,
        count(*) as variable_definition_count
    from combined_variables

),

dictionary as (

    select
        dictionary(definition) as dictionary,
        variable_definition_count
    from dictionary_definition

)

select * from dictionary
