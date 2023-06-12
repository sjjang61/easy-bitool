from fastapi import APIRouter
from fastapi.responses import JSONResponse
# from routers import action, targeting, operator, datasource
#
api_router = APIRouter(
    default_response_class=JSONResponse
)
#
# api_router.include_router(action.router, prefix='/api/v1/action', tags=["action"])
# api_router.include_router(targeting.router, prefix='/api/v1/targeting', tags=["targeting"])
# api_router.include_router(operator.router, prefix='/api/v1/operator', tags=["operator"])
#
# api_router.include_router(datasource.router, prefix='/api/v1/datasource', tags=["operator"])
