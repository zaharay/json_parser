"""
json_proc.py
Модуль получения параметров конфигурации (ссылок на ресурсы витрин) и базового класса витрины
"""

import os
import json
import pandas as pd
from log_settings import logger_config
import logging.config
import requests
from requests.exceptions import HTTPError
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from utils import get_string_intervals

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)  # запрещаю предупреждение

logger = logging.getLogger('app_logger')


def get_params(config, section='host'):
    """
    Получение списка витрин и параметров запроса, проверка URL-ов метаданных и данных, в соответствии с локализацией.

    """
    datamarts_select = str(config['datamarts']).replace(' ', '').split(',')

    data_dict = {}
    if section == 'host':
        data_dict['datamarts_path'] = config['datamarts_url']
        data_dict['metadata_path'] = config['metadata_url']
        data_dict['data_path'] = config['data_url']
        data_dict['max_rows'] = config['max_rows']
        data_dict['data_format'] = config['data_format']
        try:
            response = requests.get(data_dict['datamarts_path'], verify=False)
            # если ответ успешен, исключения задействованы не будут
            response.raise_for_status()
        except HTTPError as http_err:
            logger.exception('Ошибка HTTP: {}'.format(http_err))
            return None
        except Exception as err:
            logger.exception('Ошибка соединения:\n\t{}'.format(err))
            return None
        else:
            # logger.debug('Получен ответ от хоста с перечнем витрин: {}'.format(data_dict['datamarts_path']))
            response.encoding = 'utf-8'
            data_dict['datamarts_list'] = []
            if datamarts_select[0] != '':
                data_dict['datamarts_list'] = datamarts_select
            else:
                json_data = response.json()
                try:
                    data_dict['datamarts_list'] = json_data['DATAMARTS']
                    # data_dict['datamarts_list'] = ["/BIC/AIRSHINYL00", "/BIC/AIRSHINYF00", "/BIC/AIRFINAN00",
                    #                                "/BIC/AIRSHINY00", "/BIC/AIRSHINYB00", "/BIC/AIRSHINYD00",
                    #                                "/BIC/AIRSHINYM00"]
                except KeyError as key_err:
                    logger.exception('\nОшибка обращения к списку витрин по ключу:\n\t{}'.format(key_err))
                    return None
            return data_dict
    elif section == 'work_localhost' or section == 'home_localhost':
        path_list = config['path'].split('/')
        data_dict['datamarts_file'] = path_list[-1]
        data_dict['path'] = '/'.join(path_list[:-1])
        try:
            with open(os.path.join(data_dict['path'], data_dict['datamarts_file']),
                      'r',
                      encoding='utf-8') as json_file:
                data_dict['datamarts_list'] = []
                if datamarts_select[0] != '':
                    data_dict['datamarts_list'] = datamarts_select
                else:
                    data_dict['datamarts_list'] = json.load(json_file)['DATAMARTS']
            return data_dict
        except FileNotFoundError as file_err:
            logger.exception('\nФайл "{}" не найден'.format(data_dict['datamarts_file']))
            return None
    else:
        logger.exception('\nСекция "{}" отсутствует в config.txt!'.format(section))
        return None


def get_df_from_json(path, tag, location='host'):
    """
    Получение DataFrame (pandas) из json (по запросу от сервиса или из файла)
    """
    try:
        if location == 'host':
            response = requests.get(path, verify=False)
            response.encoding = 'utf-8'
            json_data = response.json()
        else:  # 'work_localhost' or 'home_localhost'
            with open(path, 'r', encoding='utf-8') as json_file:
                json_data = json.load(json_file)
        return pd.json_normalize(json_data['{}'.format(str(tag))])  # pd.read_json(json_data['{}'.format(str(tag))])
    except Exception as ex:
        logger.debug('\nИсключение при получении фрейма данных по адресу: {0}\n\t{1}'.format(path, ex))


# class Datamart:
#     """
#     Базовый класс витрины
#     """
#
#     def __init__(self, path_metadata, path_data, name, max_rows='30', data_format='SQL'):
#         self.path_metadata = path_metadata
#         self.path_data = path_data
#         self.name = name
#         self.max_rows = max_rows
#         self.data_format = data_format
#         self.metadata = self.get_metadata_as_df()  # pandas DataFrame
#         self.data = self.get_data_as_df()  # pandas DataFrame
#
#     def fields_format_conversion(self):
#         # Поля со значениями типа Float ('INTTYPE' == 'P')
#         float_fieldnames = self.metadata[self.metadata['INTTYPE'] == 'P']['FIELDNAME']
#         if len(float_fieldnames) > 0:
#             for fieldname in float_fieldnames:
#                 # Перенос '-' с последней позиции строкового значения на первую
#                 self.data[fieldname] = self.data[fieldname].apply(
#                     lambda x: '-' + x.replace(r'-', '') if x[-1] == '-' else x)
#                 self.data[fieldname] = self.data[fieldname].astype('float64')
#
#         # Поля со значениями типа Date ('INTTYPE' == 'D')
#         date_fieldnames = self.metadata[self.metadata['INTTYPE'] == 'D']['FIELDNAME']
#         if len(date_fieldnames) > 0:
#             for fieldname in date_fieldnames:
#                 # self.data[fieldname] = pd.to_datetime(self.data[fieldname], errors='coerce')
#                 # err_date = self.data[self.data[fieldname].isnull()].replace(pd.NaT, '')
#                 # err_date = get_string_intervals([x + 1 for x in err_date.index.to_list()])  # нумерация строк с 1
#                 # logger.debug('Витрина "{0}" - Ошибка формата/значения даты на строках: {1}'.format(self.name, err_date))
#                 self.data[fieldname] = self.data[fieldname].astype(str) # т.к. форматы даты отличаются в BW и Postgres
#
#         return self.data
#
#     def get_metadata_as_df(self):
#         """Получение фрейма метаданных витрины"""
#         try:
#             # with open(os.path.join(self.path_metadata, 'metadata_{}.json'.format(self.name)),
#             #           'r',
#             #           encoding='utf-8') as json_file:
#             #     data = json.load(json_file)
#             self.metadata = pd.json_normalize(data)  # data['METADATA'])
#             return self.metadata
#         except Exception as ex:
#             logger.debug('\nВитрина "{0}": Исключение при получении метаданных: {1}'.format(self.name, ex))
#
#     def get_data_as_df(self):
#         """Получение фрейма данных витрины """
#         try:
#             with open(os.path.join(self.path_data, 'data_{}.json'.format(self.name)),
#                       'r',
#                       encoding='utf-8') as json_file:
#                 data = json.load(json_file)
#             self.data = pd.json_normalize(data['DATA'])
#             self.data = self.fields_format_conversion()
#             return self.data
#         except Exception as ex:
#             logger.debug('Витрина "{0}": Исключение при получении данных: {1}'.format(self.name, ex))

