"""
json_proc.py
Модуль получения параметров конфигурации (ссылок на ресурсы витрин) и базового класса витрины
"""

import os
import json
import pandas as pd
from settings import logger_config
import logging.config
import requests
from requests.exceptions import HTTPError
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# import urllib.parse

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)  # запрещаю предупреждение

logger = logging.getLogger('app_logger')

def get_params(config, section='host'):
    """Получение параметров по конфигурации"""
    if section is 'work_localhost' or section is 'home_localhost':
        path_list = config['path'].split('/')
        datamarts_file = path_list[-1]
        datamarts_path = '/'.join(path_list[:-1])
        metadata_path = datamarts_path
        data_path = datamarts_path
        try:
            with open(os.path.join(datamarts_path, datamarts_file), 'r', encoding='utf-8') as json_file:
                json_data = json.load(json_file)
            return datamarts_path, metadata_path, data_path, json_data['DATAMARTS']
        except FileNotFoundError as file_err:
            logger.exception('\nФайл "{}" не найден'.format(datamarts_file))
            return None, None, None, []
    elif section is 'host':
        datamarts_path = config['datamarts_url']
        metadata_path = config['metadata_url']
        data_path = config['data_url']
        try:
            response = requests.get(datamarts_path, verify=False)
            # если ответ успешен, исключения задействованы не будут
            response.raise_for_status()
        except HTTPError as http_err:
            logger.exception('HTTP error occurred: {}'.format(http_err))
        except Exception as err:
            logger.exception('Other error occurred: {}'.format(err))
        else:
            logger.debug('\nОтвет от {}:\n\tHost response success!'.format(datamarts_path))
            response.encoding = 'utf-8'
            json_data = response.json()
            try:
                json_data = json_data['DATAMARTS']
                return datamarts_path, metadata_path, data_path, json_data
            except KeyError as key_err:
                logger.exception('\nОшибка обращения к данным по ключу:\n\t{}'.format(key_err))
                return datamarts_path, metadata_path, data_path, []
    else:
        logger.exception('\nСекция "{}" отсутствует в config.txt!'.format(section))
        return None, None, None, []


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
            logger.debug('\nВитрина "{0}" - исключение при получении метаданных: {1}'.format(self.name, ex))

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
            logger.debug('Витрина "{0}" - исключение при получении данных: {1}'.format(self.name, ex))
