with kb_string as (

    select 'sncwskzqnzduircyxlcqmtjwtpfxqknpwrxfhvkhaghqvmqputrbjymkhuwsaiemossexszuamncvovvhxvauxsgzamryugodvcfsaiyktaansnnzaeqcqemjudchmalqlowwzynuvgosiwqypldkwkmkgdqtbomwaibpfdgtkdkmtmyqfqzlolrruojuowqffbjmvxtegmwfrcsittcufzmmcojlmxwcdoggjljdpndhkkejhjklvczamqvvvcuimnmdqmqxkdsjegldbhqrnuoemqmnytsofbdmwnyfjwyuapqcyxiovjkurqflrzfxkdivorkiugtfokdozxnvqeezeknorvansrqrzqtyvxzwysxxjapoouzisikqityxagvbbfxgquefafxsjqikivgpjqnavmhjhcbiqsvttbrebxydobxlqfkrgcyjtrmifqngqmwkrikxyjkbjiuesiytvdvbbngxoudizamlisavhujagheftovpqnikdpihghhqwvmfqwyfctcmkxtxufhpqlpcxaoxvyppmurkkwdopmmkiwhacjklaomoqpndngusvtzjskxsvnmwqqdhcaqsuysvgyjajtjcvnxftzddtfgxpkcxawhbsqstmciwyujddukligvfoudcgnspptwjifvewhoetemiiuhacqnufiuipzyjpgebljtskuwpawnnfjsviwtcfaaabifrjnselmuoijpfoybebqjxxsufghsuhyupuadlksfqhuizegfzfcyeyoaugddcxrgarlsjafkidfhyslkedamgivmrpwfsjrlaoqzstjfnihkvzktdlbrwyrfzdvalvijukmfyqtivmlrtvrldalaojfvaxvuuhnmldhuzqsclkfafuyjjtqkpdbmsxbfeulpojshcyxdfiqxeoububzhlzztljsfufictfjiulpocnfbrxsqsbojkrkcgutdhrvspfajqplscvbjyoazocls' as string

),

generated_rows as (

    select
        col as row_num
    from explode(sequence(1, 10000))

),

filled_rows as (

    select
        gen.row_num,
        {% for column_number in range(1, 101) -%}
        kb.string as column_{{ column_number }}
        {%- if not loop.last %}
        ,
        {%- endif %}
        {%- endfor %}
    from generated_rows as gen, kb_string as kb

)

select * from filled_rows
