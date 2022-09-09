"""
This file contains the controller that accepts command via HTTP
and trigger business logic layer
"""
import os
from flask import Flask, request
from flask import typing as flask_typing
from helpers import convert_to_avro

app = Flask(__name__)


@app.route('/', methods=['POST', 'GET'])
def main() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts command via HTTP and
    trigger business logic layer

    Proposed POST body in JSON:
    {
      "data: "2022-08-09",
      "raw_dir": "/path/to/my_dir/raw/sales/2022-08-09"
    }
    """
    # input_data: dict = request.json
    # TODO: implement me
    # NB: you should handle the request and call these functions:
    if request.method == 'GET':
        stg_dir = request.args.get('stg_dir')
        raw_dir = request.args.get('raw_dir')
    elif request.method == 'POST':
        stg_dir = request.json.get('stg_dir')
        raw_dir = request.json.get('raw_dir')


#    print('date: ', date)
#    print('page: ', page)

    resp_convert = convert_to_avro(raw_dir, stg_dir)

    return {
               "message": resp_convert,
           }, 200


if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8082)  # Here is a Web-server by Flask on the 8082 port

