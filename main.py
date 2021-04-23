'''
How to write a simple Postgres JSON wrapper with Python:
https://levelup.gitconnected.com/how-to-write-a-simple-postgres-json-wrapper-with-python-ef09572daa66

How do I get JSON data from RESTful service using Python?:
https://stackoverflow.com/questions/7750557/how-do-i-get-json-data-from-restful-service-using-python

Read JSON response in Python:
https://stackoverflow.com/questions/33282067/read-json-response-in-python

'''

import json
import pandas as pd
from pprint import pprint
# import requests
# import urllib.parse

# (venv)>pip install flask flask-sqlalchemy
# (venv)>pip install psycopg2-binary

# main_api = 'http://maps.googleapis.com/maps/api/geocode/json?'
# address = 'lhr'
# url = main_api + urllib.parse.urlencode({'address' : address})
# print(url)

# json_data = requests.get(url).json()
# json_status = json_data['status']
# print(json_status)

# Free fake API for testing and prototyping:
# url = 'https://jsonplaceholder.typicode.com/users'
#
# response = requests.get(url).json()
# pprint(response)

table_names_file = 'c:/Users/admin/PycharmProjects/json_parser/data/TABLE_NAMES - z_iusi_ws_01.json'


with open(path + table_names_file, 'r') as json_file:
    json_data = json.load(json_file)

class JsonParser:
    def __init__(self, **args):
        self.user = args.get('user', 'postgres')
        self.port = args.get('port', 5432)
        self.dbname = args.get('dbname', 'world')
        self.host = args.get('host', 'localhost')
        self.connection = None

pprint(json_data)
table_names = [rec['NAME'] for rec in json_data['TABLE_NAMES']]
print(table_names)

#

