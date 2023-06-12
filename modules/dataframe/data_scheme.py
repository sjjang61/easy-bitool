import pandas as pd

class DataSchema():

    def __init__(self, CONFIG_PATH, filename:str ):
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

    def get_dataframe(self):
        return self.df_orig

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
