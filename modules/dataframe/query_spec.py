# 툴에서 설정하면 하단의 스키마 생성
# 날짜 연산의 경우, 상대 날짜 비교
# date 날짜 계산 (특정 컬럼 기준) : 최근 1일, 최근1주일, 최근 1달, .... : data_source 마다 문법이 다름
# 다양한 데이터 SOURCE 지원 : mysql,
query_spec_list = [
    {
        "column_list" : [ '*' ],
        "table_name" : "df_payments",
        "where" : [
            {
                "logical_oper" : "and",
                "condition" : [
                    { "column" : "amount", "oper" : ">", "value" : 10, "type" : "int" },
                    { "column" : "id", "oper" : "=", "value" : "Kim", "type" : "str"  },
                    { "column" : "date", "oper" : ">", "value" : "2015-05-18", "type" : "date" }
                ]
            }
        ],
        "group" : [],# [ 'a', 'b'],
        "having" : [],
        # [
        #     {
        #         "logical_oper" : "and",
        #         "condition" : [
        #             { "column" : "count(amount)", "oper" : ">", "value" : 10, "type" : "int" },
        #         ]
        #     }
        # ],
        "order" : [
            { "column" : "amount", "asc" : "asc" },
            { "column" : "id", "asc" : "desc" },
        ]
    }
]

def make_query(query_spec : dict):

    sql = """
    SELECT {column_list}
    FROM {table_name}        
    """.format(column_list = ", ".join(query_spec['column_list']), table_name = query_spec['table_name'])

    # where
    if len(query_spec['where']) > 0 :
        sql += "WHERE "
        for filter in query_spec['where']:
            for idx, cond in enumerate( filter['condition'] ):
                if cond['type'] == 'int':
                    sql += f"( {cond['column']} {cond['oper']} {cond['value']} )"
                else:
                    sql += f"( {cond['column']} {cond['oper']} '{cond['value']}' )"

                if idx < len( filter['condition'] ) -1 :
                    sql += f" {filter['logical_oper']} "

    # group
    if len(query_spec['group']) > 0:
        sql += f"\n GROUP BY {', '.join(query_spec['group'])}"

    # having
    if len(query_spec['having']) > 0:
        sql += "\n HAVING "
        for filter in query_spec['having']:
            for idx, cond in enumerate(filter['condition']):
                if cond['type'] == 'int':
                    sql += f"( {cond['column']} {cond['oper']} {cond['value']} )"
                else:
                    sql += f"( {cond['column']} {cond['oper']} '{cond['value']}' )"

                if idx < len(filter['condition']) - 1:
                    sql += f" {filter['logical_oper']} "

    # order
    if len(query_spec['order']) > 0:
        sql += "\n ORDER BY "
        for idx, order in enumerate(query_spec['order']):
            sql += f"{order['column']} {order['asc']}"
            if idx < len (query_spec['order']) - 1:
                sql += ', '


    return sql


def make_table_query(query_spec_list : list):
    sql_list = []
    for query_spec in query_spec_list:
        # print( table_spec )
        sql = make_query( query_spec )
        sql_list.append( sql )
        print( sql )

    return sql_list

# make_table_query( query_spec_list )