import time
import logging

from fastapi import Request, APIRouter, Path
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

logger = logging.getLogger(__file__)
router = APIRouter(
    # prefix="/api/v1/operator",
    # tags=["operator"],
#     # dependencies=[Depends(get_token_header)],
    responses={404: {"description": "Not found"}},
)

def make_response( is_validate, message, output ):

    response = {
        "is_validate" : is_validate,
        "message" : message,
        "output" : output
    }
    return JSONResponse(content=jsonable_encoder(response))


