import logging
import requests
import json

logger = logging.getLogger(__file__)

def get( url, data = {}, headers = {}, timeout=10):
    # headers = {'Content-Type': 'application/json; charset=utf-8'}
    try:
        response = requests.get(url=url, data=data, headers=headers, timeout=timeout )
        logger.debug("[API-GET-RES] status = %s, response = %s" % (response.status_code, response.json()))
    except Exception as ex:
        logger.error(f"[Exception] http-method = get, url = {url}, message = {ex}")
        raise

    return response.json()

def get_text( url, data = {}, headers = {}, timeout=10):
    try:
        response = requests.get(url=url, data=data, headers=headers, timeout=timeout)
        logger.debug("[API-GET-RES] status = %s, response = %s" % ( response.status_code, response.text ))
    except Exception as ex:
        logger.error(f"[Exception] http-method = get, url = {url}, message = {ex}")
        raise
    return response.text

def post( url, data, headers, timeout=10, is_response_json = True ):
    try:
        response = requests.post(url=url, data=json.dumps(data), headers=headers, timeout=timeout)
        if is_response_json == True:
            logger.debug("[API-POST-RES] status = %s, response = %s" % ( response.status_code, response.json() ))
        else:
            logger.debug("[API-POST-RES] status = %s, response = %s" % (response.status_code, response.text ))
    except Exception as ex:
        logger.error(f"[Exception] http-method = post, url = {url}, message = {ex}")
        raise

    if is_response_json == True:
        return response.json()

    return response.text

def put( url, data, headers, timeout=10):
    response = requests.put(url=url, data=json.dumps(data), headers=headers, timeout=timeout)
    logger.debug("[API-PUT-RES] status = %s, response = %s" % ( response.status_code, response.json() ))
    return response.json()

def delete( url, data, headers, timeout=10 ):
    response = requests.delete(url=url, data=json.dumps(data), headers=headers, timeout=timeout)
    logger.debug("[API-DELETE-RES] status = %s, response = %s" % ( response.status_code, response.json() ))
    return response.json()