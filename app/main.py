from fastapi import FastAPI, UploadFile, File, status
from fastapi.responses import JSONResponse
from pathlib import Path
from modules.utils.custom_logging import CustomizeLogger
import config
import logging
from app.api import api_router
# import aiofiles
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

# pip install aiofiles, jinsa2
# https://github.com/Tinche/aiofiles

from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request
from modules.dataframe import query_spec, table_join_spec

logger = logging.getLogger(__name__)
config_path = Path(__file__).with_name("logging_config.json")


from pandasql import sqldf
dfsql = lambda q: sqldf(q, globals())
dfsql_local = lambda q: sqldf(q, locals())


def create_app() -> FastAPI:
    _app = FastAPI(title='easy-bitool', debug=False, docs_url="/documentation", redoc_url=None )
    _app.logger = CustomizeLogger.make_logger(config_path, config.settings.env)
    logger.info('APP_ENV: {}'.format(config.settings.env))
    return _app

# app = FastAPI()
# app = FastAPI(dependencies=[Depends(get_query_token)])
templates = Jinja2Templates(directory="templates")

app = create_app()
app.include_router(api_router)


from config import CONFIG_PATH
from modules.dataframe.data_scheme import DataSchema

@app.get("/")
def index(request: Request):

    # 사용자별로 세션별로 데이터 관리 필요
    data_scheme = DataSchema( CONFIG_PATH, 'payment.csv')
    response = {
        "header": data_scheme.df_orig.columns.tolist(),
        "body": list(data_scheme.df_orig.T.to_dict().values()),
        "meta_info_list" : data_scheme.meta_header_list
    }
    print( response )
    # # 원본
    # print(payments)
    # # 가공
    # df_payments = payments.add_columns('amount2', 'amount', 'plus', 1)
    # df_payments = payments.add_columns('result2', 'result', 'plus', '_abc')
    # df_payments = payments.add_columns('date2', 'date', 'plus', 1)
    # print(df_payments.head())

    return templates.TemplateResponse("index.html", context={"request": request, "response" : response })

# global df_user, df_payments, df_item

@app.get("/index2")
def index2(request: Request):

    # 사용자별로 세션별로 데이터 관리 필요
    data_scheme = DataSchema( CONFIG_PATH, 'payment.csv')
    response = {
        "header": data_scheme.df_orig.columns.tolist(),
        "body": list(data_scheme.df_orig.T.to_dict().values()),
        "meta_info_list" : data_scheme.meta_header_list
    }

    # df_payments = data_scheme.df_orig
    df_payments = data_scheme.get_dataframe()
    sql_list = query_spec.make_table_query( query_spec.query_spec_list )

    for sql in sql_list:
        print( sql )
        try:
            # df = execute_dataframe_sql( sql )
            df_res = sqldf( sql, locals())
            print( df_res.head())
        except Exception as ex:
            logger.error(f"[Exception] message = {ex}")

    return templates.TemplateResponse("index.html", context={"request": request, "response" : response })


@app.get("/index3")
def index3(request: Request):

    payments = DataSchema( CONFIG_PATH, 'payment.csv')
    # 원본
    print(payments)
    # 가공
    df_payments = payments.add_columns('amount2', 'amount', 'plus', 1)
    df_payments = payments.add_columns('result2', 'result', 'plus', '_abc')
    df_payments = payments.add_columns('date2', 'date', 'plus', 1)
    print(df_payments.head())
    print("\n\n")

    user = DataSchema(CONFIG_PATH, 'user.csv')
    df_user = user.df_orig
    print(user)

    item = DataSchema(CONFIG_PATH, 'item.csv')
    df_item = item.df_orig
    print(item)

    sql = table_join_spec.make_query_inner_join("df_user", "df_payments", "id", "id")

    print( sql )
    print("======[ inner join ]======")
    df_res = sqldf( sql, locals())
    print(df_res.columns)

    response = {
        "header": df_res.columns.tolist(),
        "body": list(df_res.T.to_dict().values()),
        # "meta_info_list": data_scheme.meta_header_list
    }

    return templates.TemplateResponse("index.html", context={"request": request, "response" : response })




@app.get("/load/{filename}")
def load_data(request: Request, filename: str ):
    print( "load file : ", filename )

    data_scheme = DataSchema(CONFIG_PATH, filename )
    response = {
        "header": data_scheme.df_orig.columns.tolist(),
        "body": list(data_scheme.df_orig.T.to_dict().values()),
        "meta_info_list": data_scheme.meta_header_list
    }
    return JSONResponse( content=jsonable_encoder(response) )

def execute_dataframe_sql( sql, limit=5 ):

    try:
        df_res = dfsql(sql)
        # df_res = dfsql_local(sql)
        print( df_res.shape )
        print( df_res.head(limit))
        return df_res
    except Exception as ex:
        logger.error(f"[Exception] message = {ex}")

    return None
