"""
This file contains the controller that accepts command via HTTP
and trigger business logic layer
"""
import os
from flask import Flask, request, jsonify
from flask import typing as flask_typing
from helpers import get_sales, save_to_disk, AUTH_TOKEN


if not AUTH_TOKEN:
    print("AUTH_TOKEN environment variable must be set")


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
        date = request.args.get('date')
        raw_dir = request.args.get('raw_dir')
    elif request.method == 'POST':
        date = request.json.get('date')
        raw_dir = request.json.get('raw_dir')


#    print('date: ', date)
#    print('page: ', page)

    resp_sales = get_sales(date, raw_dir)

    return {
               "message": resp_sales,
           }, 200


if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8081)  # Here is a Web-server by Flask on the 8081 port

