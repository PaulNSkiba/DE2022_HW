import os
import shutil
import json
import requests

AUTH_TOKEN = os.environ.get("API_AUTH_TOKEN")       # Secret token from the environment
URI = 'https://fake-api-vycpfa6oca-uc.a.run.app/'   # Path to API


def get_sales(date, raw_dir=''):
    page = 1
    actual_raw_dir = os.getcwd().replace('\\job1', '') + '\\file_storage' + raw_dir

    if os.path.exists(actual_raw_dir):
        shutil.rmtree(actual_raw_dir)

    os.makedirs(actual_raw_dir)

    while True:
        response = requests.get(
            url=f'{URI}sales?date={date}&page={page}',
            headers={'Authorization': AUTH_TOKEN}
        )

        if response.status_code == 200:
            resp_of_save = save_to_disk(actual_raw_dir + '/' + 'sales_' + str(date) + '_' + str(page) + '.json', response.json())
            page += 1
        else:
            page -= 1
            break

    #    print("Response status code:", response.status_code)
    # return response.json()
    return 'Got and Saved files: ' + str(page)


def save_to_disk(file_to_storage='', response={}):
    print('save to disk')
    with open(file_to_storage, 'w') as outfile:
        json.dump(response, outfile)

    return True
