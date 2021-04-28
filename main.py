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
# path = 'd:/Git/Python/json_parser/data'

datamarts_list_file = 'Datamarts - Z_IUSI_WS_DATAMARTS.json'  # список
matadata_AIRSHINYL00_file = 'Metadata - /BIC/AIRSHINYL00 - z_iusi_ws_metadata.json'  # метаданные витрины AIRSHINYL00

class Datamart:
    """Базовый класс витрины"""

    def __init__(self, path, name):
        self.path = path
        self.name = name
        self.metadata = None

    def get_metadata_as_df(self):
        """Получение метаданных витрины"""
        try:
            with open(os.path.join(self.path, 'data_{}.json'.format(self.name)), 'r') as json_file:
                data = json.load(json_file)
            # pprint(data)
            self.metadata = pd.json_normalize(data['DATA'])
        except Exception as ex:
            print('Витрина {0} - исключение при получении метаданных: {}'.format(self.name, ex))


# Метаданные витрины '/BIC/AIRSHINYL00' <- УТОЧНИТЬ в URL!!!:


# Pandas DataFrame:
# df = pd.json_normalize(metadata)
# print(df)
#
# df.to_csv(os.path.join(path, 'metadata.csv'), sep='\t', encoding='utf-8', index=False)
#
# # pprint(metadata)
# fieldnames = [rec['FIELDNAME'] for rec in metadata]

# Список витрин:
# with open(os.path.join(path, datamarts_list_file), 'r') as json_file:
#     json_data = json.load(json_file)
# datamarts = json_data['DATAMARTS']

# print('Список витрин:', datamarts)
# print('Список полей витрины "/BIC/AIRSHINYL00":', fieldnames, len(fieldnames))

dm1 = Datamart(r'c:/Users/admin/PycharmProjects/json_parser/data/', 'AIRSHINYL00')
dm1.get_metadata_as_df()
print(dm1.metadata)