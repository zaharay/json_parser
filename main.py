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

path = 'c:/Users/admin/PycharmProjects/json_parser/data/'
table_names_file = 'TABLE_NAMES - z_iusi_ws_01.json'

with open(path + table_names_file, 'r') as json_data:
    # use load() rather than loads() for JSON files
    record_list = json.load(json_data)

pprint(record_list)
table_names = record_list['TABLE_NAMES']
df = pd.DataFrame(table_names)
print(df['NAME'][0])

# df_tables = pd.read_json(record_list['TABLE_NAMES'], orint='records'   )
# print(df_tables)

