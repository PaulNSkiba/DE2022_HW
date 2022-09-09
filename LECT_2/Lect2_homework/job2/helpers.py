import os
import shutil

import requests
from fastavro import json_writer, parse_schema, writer
import json


def convert_to_avro(raw_dir, stg_dir):
    actual_raw_dir = os.getcwd().replace('\\job2', '') + '\\file_storage' + raw_dir
    actual_stg_dir = os.getcwd().replace('\\job2', '') + '\\file_storage' + stg_dir

    if os.path.exists(actual_stg_dir):
        shutil.rmtree(actual_stg_dir)

    os.makedirs(actual_stg_dir)

    schema = parse_schema({
      "name": "JustSchema",
      "type": "record",
      "namespace": "com.acme.avro",
      "fields": [
        {
          "name": "client",
          "type": "string"
        },
        {
          "name": "price",
          "type": "int"
        },
        {
          "name": "product",
          "type": "string"
        },
        {
          "name": "purchase_date",
          "type": "string"
        }
        ]})

    for f in os.listdir(actual_raw_dir):
        if f.endswith(".json"):
            with open(actual_raw_dir + '/' + f) as js:
                json_file = json.load(js)

            with open(actual_stg_dir + '/' + f.replace('.json', '.avro'), 'wb') as out:
                writer(out, schema, json_file, codec='deflate')
            # print(actual_raw_dir, f)

    return 'Avro files saved'
