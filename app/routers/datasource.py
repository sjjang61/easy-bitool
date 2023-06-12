import logging
import copy
import pandas as pd
from fastapi import Request, APIRouter
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from config import CONFIG_PATH

logger = logging.getLogger(__file__)
router = APIRouter(
    responses={404: {"description": "Not found"}},
)

@router.get("/", summary="전체 action 조회")
async def select_list():
    print("select_data_source")


    df = pd.read_csv( CONFIG_PATH + '/payment.csv', sep="\t" )
    # df = pd.read_csv(args.input, sep=args.delimeter, names=header_list, dtype=dtype)