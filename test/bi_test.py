import logging
import pandas as pd
import json
# from . import table_spec
# import table_spec
# import table_join_spec

from modules.dataframe import table_join_spec
from modules.dataframe import query_spec

from config import CONFIG_PATH
from pandasql import sqldf

dfsql = lambda q: sqldf(q, globals())
# dfsql_local = lambda q: sqldf(q, locals())

def dfsql_local( sql ):
    return sqldf( sql, locals())


## pip install -U pandasql

class DataSchema():

    def __init__(self, filename:str ):
        self.filename = filename
        self.df_orig = pd.read_csv( CONFIG_PATH + '/' + filename, sep="," )
        self.df_edit = self.df_orig.copy()
        self.header_list = self.df_orig.columns.tolist()
        self.data = list(self.df_orig.T.to_dict().values())

        self.meta_header_list = []
        self.make_meta_info()

    def __repr__(self):
        print( self.df_orig.head() )
        return self.filename

    def make_meta_info( self ):
        for value in self.header_list:
            meta_header = {
                'key': value,
                'is_key': (value == 'id'),    # 외부에서 설정
                'is_date': (value == 'date'),   # 외부에서 설정
                'date_format': '%Y-%m-%d' if value == 'date' else None,
                'type': str( self.df_orig.dtypes[value])
            }
            self.meta_header_list.append(meta_header)
            # print(meta_header)

        # print( meta_header_list )

    def get_meta_info( self, column_name : str ):
        for meta in self.meta_header_list:
            if meta['key'] == column_name :
                return meta

        return None

    def add_columns( self, new_column_name, column_name, operator, value):

        meta_info = self.get_meta_info(column_name)
        if meta_info['type'] == 'object' and meta_info['is_date'] == True:      # date
            self.df_edit[new_column_name] = pd.to_datetime( self.df_edit[column_name]) + pd.Timedelta(days=value)
        elif meta_info['type'] == 'object' and meta_info['is_date'] == False:   # string
            self.df_edit[new_column_name] = self.df_edit[column_name] + value
        elif meta_info['type'].startswith('int'):                               # int
            self.df_edit[new_column_name] = self.df_edit[column_name] + value

        return self.df_edit

    # def get_dataframe(self, sql : str, limit : int = 5 ):
    #
    #     temp = self.df_orig.copy()
    #     print( "sql = ", sql.format( table_name = 'temp'))
    #     df_res = dfsql( sql.format( table_name = 'temp') )
    #     print(df_res.shape)
    #     print(df_res.head(limit))
    #     return df_res


def load( file_name ):
    df = pd.read_csv( CONFIG_PATH + '/' + file_name, sep="," )
    print( df.head())
    # print( df.columns.tolist() )
    # print( df.dtypes['Name'] )

    header, data = make_header_data(df)
    meta_header_list = make_meta_columns(df, header, data)

    return df, header, data, meta_header_list


def make_header_data( df : pd.DataFrame ):
    response = {
        "header": df.columns.tolist(),
        "data": list(df.T.to_dict().values())
    }

    json_formatted_str = json.dumps(response, indent=4, ensure_ascii=False)
    print( json_formatted_str )
    return response['header'], response['data']

def make_meta_columns( df : pd.DataFrame, header_list : list , data ):

    meta_header_list = []
    for value in header_list:
        meta_header = {
            'key': value,
            'is_key': ( value == 'Name'),
            'is_date': ( value == 'date' ),
            'date_format' : '%Y-%m-%d' if value == 'date' else None,
            'type' : str( df.dtypes[value])
        }
        meta_header_list.append( meta_header )
        print( meta_header )

    # print( meta_header_list )
    return meta_header_list

def get_meta_columns( column_name : str ):
    # for meta in meta_header_list:
    #     # print( "[META DEF] ", meta )
    #     if meta['key'] == column_name :
    #         return meta

    return None

def add_columns( df, new_column_name, column_name, operator, value ):

    meta_info = get_meta_columns( column_name )
    if meta_info['type'] == 'object' and meta_info['is_date'] == True:
        df[new_column_name] = pd.to_datetime(df[column_name] ) + pd.Timedelta(days= value)
    elif meta_info['type'] == 'object' and meta_info['is_date'] == False:
        df[new_column_name] = df[column_name] + value
    elif meta_info['type'].startswith( 'int'):
        df[new_column_name] = df[column_name] + value
    return df


def execute_df_sql( sql, limit=5 ):
    df_res = dfsql(sql)
    # df_res = dfsql_local(sql)
    print( df_res.shape )
    print( df_res.head(limit))
    return df_res

# df, header, data, meta_header_list
# df_payments = load( 'payment.csv')
# df_user = load( 'user.csv')

def make_query_inner_join( table1, table2, table1_column, table2_column = '' ):

    sql = """
        select a.*, b.*
        from {table_name1} a
        inner join {table_name2} b on ( a.{table1_column} = b.{table2_column} )
    """.format( table_name1 = table1, table_name2 = table2, table1_column = table1_column, table2_column = table2_column )
    return sql

def make_query_join_key( join_key_list : list ):
    condition = ''
    for idx, cond in enumerate( join_key_list ):
        if idx == 0 :
            condition = f"\nINNER JOIN {cond['table_name']} ON {cond['table_name']}.{cond['key']} = "
        else:
            condition += f"{cond['table_name']}.{cond['key']}"

    return condition

def make_query_inner_join2( dt_join_codition : dict ):

    template_sql = """
        SELECT {table_list_str}.*
        FROM {table1}          
    """.format( table_list_str = ".*, ".join(dt_join_codition['table_list']), table1 = dt_join_codition['table_list'][0] )
    print(template_sql)

    condition = ''
    # for idx, cond in enumerate( dt_join_codition['cond1'] ):
    #     if idx == 0 :
    #         condition = f"ON {cond['table_name']}.{cond['key']} = "
    #     else:
    #         condition += f"{cond['table_name']}.{cond['key']}"

    condition += make_query_join_key( dt_join_codition['cond1'] )
    condition += make_query_join_key( dt_join_codition['cond2'])
    condition += make_query_join_key( dt_join_codition['cond3'])
    print( condition )


    # return sql



table_join_condition = {
    "table_list" : ["df_user", "df_payments", "df_item"],
    "cond1" : [
        { 'table_name' : 'df_user', 'key' : 'id' },
        { 'table_name' : 'df_payments', 'key' : 'id' },
    ],
    "cond2" :[
        { 'table_name' : 'df_payments', 'key' : 'item_id' },
        { 'table_name' : 'df_item', 'key' : 'item_id' },
    ],
    "cond3" :[
    ],
}


def main():

    payments = DataSchema('payment.csv')
    df_payments = payments.df_orig
    # 원본
    print(payments)

    # 가공
    # df_payments = payments.add_columns('amount2', 'amount', 'plus', 1)
    # df_payments = payments.add_columns('result2', 'result', 'plus', '_abc')
    # df_payments = payments.add_columns('date2', 'date', 'plus', 1)
    print(df_payments.head())

    print("======[ panddas sql ]======")
    df_res = sqldf("SELECT * FROM df_payments", locals())
    print( df_res.head() )
    # print( dfsql_local( "SELECT * FROM df_payments" ) )
    return
    print("\n\n")

    user = DataSchema('user.csv')
    df_user = user.df_orig
    print(user)

    item = DataSchema('item.csv')
    df_item = item.df_orig
    print(item)

    # df2 = df.copy()
    # df2 = add_columns( df2, 'amount2', 'amount', 'plus', 1 )
    # df2 = add_columns( df2, 'result2', 'result', 'plus', '_abc' )
    # df2 = add_columns( df2, 'date2', 'date', 'plus', 1 )
    # print( df2.head() )


    sql = make_query_inner_join( "df_user", "df_payments", "id", "id" )
    print("======[ inner join ]======")
    df_res = execute_df_sql( sql )
    print( df_res.columns )


    make_query_inner_join2( table_join_condition )


    table_spec_list = [
        {
            "column_list" : [ 'date', 'sum(amount) as sum' ],
            "table_name" : "df_payments",
            "where" : [
                {
                    "logical_oper" : "and",
                    "condition" : [
                        { "column" : "amount", "oper" : ">", "value" : 10, "type" : "int" },
                    ]
                }
            ],
            "group" : [ 'date' ],
            "having" : [],
            "order" : [],
        }
    ]

    table_spec_list = [
        {
            "column_list" : [ 'date', 'sum(amount) as sum' ],
            "table_name" : "df_payments",
            "where" : [
                {
                    "logical_oper" : "and",
                    "condition" : [
                        { "column" : "amount", "oper" : ">", "value" : 10, "type" : "int" },
                    ]
                }
            ],
            "group" : [ 'date' ],
            "having" : [
                {
                    "logical_oper" : "and",
                    "condition" : [
                        { "column" : "sum(amount)", "oper" : ">", "value" : 600, "type" : "int" },
                        { "column" : "sum(amount)", "oper" : ">=", "value" : 700, "type" : "int" },
                    ]
                }
            ],
            "order" : [
                { "column" : "sum", "asc" : "asc" },
            ]
        }
    ]

    sql_list = query_spec.make_table_query( table_spec_list )
    # print( sql_list )
    df_res = execute_df_sql( sql_list[0] )
    print( df_res.columns )


    # join (중복 컬럼 제거)
    sql = table_join_spec.make_query_join( table_join_spec.join_condition )
    df_res = execute_df_sql( sql )
    columns = set( df_res.columns.to_list())
    print( df_res.T.duplicated() )  # ROW 중복체크

    # df_res2 = df_res.T.drop_duplicates().T  # col 중복 삭제후 다시 원상복귀
    df_res2 = df_res.loc[ :, ~df_res.T.duplicated() ]
    print( df_res2.columns )
    print( df_res2.head())


if __name__ == '__main__':
    main()
