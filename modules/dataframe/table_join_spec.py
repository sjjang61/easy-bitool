# table_list 의 첫번째가 driving table
# driving table(outer table)
# driven table(inner table)
join_condition = {
    "table_list" : ["df_user", "df_payments", "df_item"],
    "condition" : [
        {
            "join_type" : "INNER",
            "driving" : { 'table_name' : 'df_user', 'key' : 'id' },
            "driven" : { 'table_name' : 'df_payments', 'key' : 'id' },
        },
        {
            "join_type" : "OUTER LEFT",
            "driving" : { 'table_name' : 'df_payments', 'key' : 'item_id' },
            "driven" : { 'table_name' : 'df_item', 'key' : 'item_id' },
        },
    ]
}

def make_query_join_key( join_key_list : list ):
    condition = ''
    for idx, cond in enumerate( join_key_list ):
        condition += f"\n{cond['join_type']} JOIN {cond['driven']['table_name']} ON {cond['driven']['table_name']}.{cond['driven']['key']} = {cond['driving']['table_name']}.{cond['driving']['key']}"

    return condition

def make_query_join( dt_join_codition : dict ):

    join_condition = make_query_join_key(dt_join_codition['condition'])
    print(join_condition)

    template_sql = """
        SELECT {table_list_str}.*
        FROM {table1}
        {join_condition}          
    """.format( table_list_str = ".*, ".join(dt_join_codition['table_list']), table1 = dt_join_codition['table_list'][0], join_condition = join_condition )
    print(template_sql)


    return template_sql


def make_query_inner_join( table1, table2, table1_column, table2_column = '' ):

    sql = """
        select a.*, b.*
        from {table_name1} a
        inner join {table_name2} b on ( a.{table1_column} = b.{table2_column} )
    """.format( table_name1 = table1, table_name2 = table2, table1_column = table1_column, table2_column = table2_column )
    return sql


# make_query_join( join_condition )