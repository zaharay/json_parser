"""
How to write a simple Postgres JSON wrapper with Python:
https://levelup.gitconnected.com/how-to-write-a-simple-postgres-json-wrapper-with-python-ef09572daa66

How do I get JSON data from RESTful service using Python?:
https://stackoverflow.com/questions/7750557/how-do-i-get-json-data-from-restful-service-using-python

Read JSON response in Python:
https://stackoverflow.com/questions/33282067/read-json-response-in-python
"""

import os
import json
import pandas as pd
from settings import logger_config
import logging.config
# import requests
# import urllib.parse

# main_api = 'http://maps.googleapis.com/maps/api/geocode/json?'
# address = 'lhr'
# url = main_api + urllib.parse.urlencode({'address' : address})
# print(url)

# json_data = requests.get(url).json()
# json_status = json_data['status']
# print(json_status)

logger = logging.getLogger('app_logger')

def get_datamarts_list(datamarts_path):
    """Получение списка витрин"""
    try:
        with open(datamarts_path, 'r', encoding='utf-8') as json_file:
            json_data = json.load(json_file)
        return json_data['DATAMARTS']
    except Exception as ex:
        logger.exception(ex)


class Datamart:
    """Базовый класс витрины"""

    def __init__(self, path_metadata, path_data, name):
        self.path_metadata = path_metadata
        self.path_data = path_data
        self.name = name
        self.metadata = None  # pandas DataFrame
        self.data = None  # pandas DataFrame

    def get_metadata_as_df(self):
        """Получение фрейма метаданных витрины"""
        try:
            with open(os.path.join(self.path_metadata, 'metadata_{}.json'.format(self.name)),
                      'r',
                      encoding='utf-8') as json_file:
                data = json.load(json_file)
            # pprint(data)
            self.metadata = pd.json_normalize(data['METADATA'])
            return self.metadata
        except Exception as ex:
            logger.debug('Витрина "{0}" - исключение при получении метаданных: {1}'.format(self.name, ex))

    def get_data_as_df(self):
        """Получение фрейма данных витрины """
        try:
            with open(os.path.join(self.path_data, 'data_{}.json'.format(self.name)),
                      'r',
                      encoding='utf-8') as json_file:
                data = json.load(json_file)
            self.data = pd.json_normalize(data['DATA'])
            return self.data
        except Exception as ex:
            print('Витрина "{0}" - исключение при получении данных: {1}'.format(self.name, ex))
