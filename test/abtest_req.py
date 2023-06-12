import requests

def get( url, data = {}, headers = {}):
    # headers = {'Content-Type': 'application/json; charset=utf-8'}
    response = requests.get(url=url, data=data, headers=headers)
    # log.debug("[API-GET-RES] status = %s, response = %s" % (response.status_code, response.json()))
    # print("[API-GET-RES] status = %s, response = %s" % (response.status_code, response.json()))
    return response.json()

# curl -H "svcCd: apollo" -H "funcCd: llm" -H "userId: user10" -H "dialogRequestId: 1af0b64ae90110d5a7061d0761678875"
# -X GET "http://172.27.31.228:29060/api/v1/abtest/"
headers = {
    'svcCd': 'apollo',
    'funcCd': 'llm',
    'dialogRequestId': '1af0b64ae90110d5a7061d0761678875',
}
url = "http://172.27.31.228:29060/api/v1/abtest/"

if __name__ == "__main__":
    req_cnt = 10000
    test_cnt = 0
    for i in range(req_cnt):
        print(i)
        # headers['userId'] = ( 'user%05d' % i )
        headers['userId'] = ('user%d' % i)
        # print( headers )
        res = get( url, data={}, headers=headers )
        if res['body'][0]['experimentGroup']['groupCd'] == 'llm39':
            print("\t llm39")
            test_cnt += 1

    print( f"total_cnt = {req_cnt}, test_cnt = {test_cnt}, rate = {test_cnt/req_cnt}")
