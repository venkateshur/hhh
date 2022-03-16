

import json
import requests


def get_call(end_point, params, headers):
    response = requests.get(end_point, params=params, headers=headers, verify=False)
    no_of_retries = 3
    while response.status_code != 200 and no_of_retries > 0:
        response = requests.get(end_point, params=params, headers=headers, verify=False)
        no_of_retries -= 1

    if response.status_code != 200:
        raise Exception("Unable to fetch the encrypted values for repids. Error is {response.json()}")

    return response.json()


def get_entities_count(end_point, params, headers):
    return json.loads(get_call(end_point, params, headers))['value']


def get_encrypted_df(spark, col_to_encrypt, api_key):
    CLIENT_IDS_MAX_COUNT_END_POINT_URL = ""
    RAPID_IDS_MAX_COUNT_END_POINT_URL = ""
    CLIENT_IDS_END_POINT_URL = ""
    RAPID_IDS_END_POINT_URL = ""

    params = [('keytype', col_to_encrypt)]
    headers = {'x-api-key': api_key}

    if col_to_encrypt == 'repid':
        max_count = get_entities_count(RAPID_IDS_MAX_COUNT_END_POINT_URL, params, headers)
    else:
        max_count = get_entities_count(CLIENT_IDS_MAX_COUNT_END_POINT_URL, params, headers)

    if col_to_encrypt == 'repid':
        ALL_TOKENS_ENDPOINT_URL = RAPID_IDS_END_POINT_URL
    else:
        ALL_TOKENS_ENDPOINT_URL = CLIENT_IDS_END_POINT_URL

    responses_list = []
    if max_count < 100:
        response = get_call(ALL_TOKENS_ENDPOINT_URL, params, headers)
    else:
        incremental_count = 0
        for offset in range(0, max_count, 100):
            incremental_count = i + 1
            responses_list.append(get_call(ALL_TOKENS_ENDPOINT_URL + "/ofsset=" + offset, params, headers))

        if (incremental_count < max_count) and (incremental_count + 100 <= max_count):
            responses_list.append(get_call(ALL_TOKENS_ENDPOINT_URL + "/ofsset=" + offset, params, headers))

    if col_to_encrypt == 'repid':
        schema = ['REP_ID', 'TokenizedRepID']

    if col_to_encrypt == 'clientid':
        schema = ['CLIENT_ID', 'TokenizedClientID']

    encrypted_df = spark.createDataFrame(data=response.json(), schema=schema)

    return encrypted_df
