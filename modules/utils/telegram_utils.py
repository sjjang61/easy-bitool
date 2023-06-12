import logging
from . import http_utils
logger = logging.getLogger(__file__)

def send_message(url : str, message : str , token : str):
    """
    T전화에서 사용하는 텔레그램 프록시 서버로 메시지 전송
    stg, prd 환경에서만 발송가능
    """
    if url.strip() in [ '', 'none']:
        return

    data = {
        "text" : message,
        "token" : token,
        "chat_type": "CRM_PUSH",
    }
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    http_utils.post( url, data, headers, timeout=10, is_response_json = False )