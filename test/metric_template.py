from abc import *


class MetricTemplate(object):
    __metaclass__ = ABCMeta

    def __init__(self, table_name ):
        self.table_name = table_name

    @abstractmethod
    def get_query(self):
        pass


class UserTemplate(MetricTemplate):

    def __init__(self, table_name, table_spec ):
        self.table_name = table_name
        self.table_spec = table_spec

    def get_query(self):
        sql = """
            SELECT join_date, count(*) join_cnt
            FROM df_user
            GROUP BY join_date            
        """
        return sql

class RetentionTemplate(MetricTemplate):

    def __init__(self, table_name, table_spec ):
        self.table_name = table_name
        self.table_spec = table_spec

    def get_query(self):
        sql = """
            SELECT 
                join_date, ( usg.usg_date - usr.join_date ) gap, user_id                
            FROM
            (
                SELECT join_date, user_id
                FROM df_user
                WHERE join_date >= '2023-05-15'
            ) usr,
            (
                SELECT usg_date, user_id
                FROM df_action
                GROUP BY usg_date, user_id
            ) usg
            WHERE usr.user_id = usg.user_id
        """
        return sql
