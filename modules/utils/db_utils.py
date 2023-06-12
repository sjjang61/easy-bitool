# import pymysql
import logging
import pandas as pd
from modules.connector.mysql_connector import MySQLConnector
from modules.settings.settings import environment

logger = logging.getLogger(__file__)
mysql_connector = MySQLConnector( environment )

def get_db_conn():

    # return pymysql.connect(host=db_info['host'],
    #                        user=db_info['user'],
    #                        port=db_info['port'],
    #                        passwd=db_info['passwd'],
    #                        db=db_info['db'],
    #                        charset="utf8")
    return mysql_connector.conn

def write_db( sql ):
    # cursor = db_conn.cursor()
    # rows = cursor.execute(sql)
    # db_conn.commit()
    # return rows

    mysql_connector.runquery_commit(sql)
    logger.info("Database Commit...")



def execute_db( sql ):
    # return pd.read_sql(sql, db_conn)
    df = mysql_connector.get_dataframe( sql )
    if isinstance(df, pd.DataFrame) and not df.empty:
        return df

    return pd.DataFrame()
