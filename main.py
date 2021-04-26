'''
How to write a simple Postgres JSON wrapper with Python:
https://levelup.gitconnected.com/how-to-write-a-simple-postgres-json-wrapper-with-python-ef09572daa66

How do I get JSON data from RESTful service using Python?:
https://stackoverflow.com/questions/7750557/how-do-i-get-json-data-from-restful-service-using-python

Read JSON response in Python:
https://stackoverflow.com/questions/33282067/read-json-response-in-python

'''
import os
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

# path = 'c:/Users/admin/PycharmProjects/json_parser/data/'
path = 'd:/Git/Python/json_parser/data'
table_names_file = 'TABLE_NAMES - z_iusi_ws_01.json'  # список таблиц
datamarts_list_file = 'Showcase list - Z_IUSI_WS_DATAMARTS.json'  # список
matadata_file = 'Metadata - z_iusi_ws_metadata.json'  # метаданные

# Наименования таблиц:
with open(os.path.join(path, table_names_file), 'r') as json_file:
    json_data = json.load(json_file)
table_names = [rec['NAME'] for rec in json_data['TABLE_NAMES']]

# Список витрин:
with open(os.path.join(path, datamarts_list_file), 'r') as json_file:
    json_data = json.load(json_file)
datamarts = json_data['DATAMARTS']

# Метаданные витрины '/BIC/AIRSHINYL00' <- УТОЧНИТЬ в URL!!!:
with open(os.path.join(path, matadata_file), 'r') as json_file:
    json_data = json.load(json_file)
metadata = json_data['METADATA']

# Pandas DataFrame:
df = pd.json_normalize(metadata)
print(df)

# pprint(metadata)
fieldnames = [rec['FIELDNAME'] for rec in metadata]


print('Список таблиц:', table_names)
print('Список витрин:', datamarts)
print('Список полей витрины "/BIC/AIRSHINYL00":', fieldnames, len(fieldnames))