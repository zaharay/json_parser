"""
utils.py
Модуль вспомогательных инструментов:
 * config_parser() - парсер файла конфигурации
 * write_csv() - запись фрейма данных в csv-файл
 * replace_list_by_dict() - замена элементов списка значениями из словаря (по ключу)
"""


import pandas as pd
import configparser

def config_parser(file_path, section='host'):
    """
    Парсер файла конфигурации
    :param config_path: путь к файлу конфигурации
    :return: словарь с URL-адресами ресурсов (списка витрин, метаданных витрин, данных)
    """
    # with open(file_path, 'r') as file:
    #     config = dict()
    #     lines = file.readlines()
    #     for line in lines:
    #         k, v = line.split(' = ')
    #         config[k] = v
    #     return config
    config = configparser.ConfigParser()
    config.read(file_path)
    result = dict()
    for key in config[section]:
        result[key] = config[section][key]
    return result


def write_csv(filename, data):
    """
    Запись данных в CSV-файл
    :param filename: файл для записи
    :param data: данные (pandas DataFrame) для
    :return: 0 - ок, 1 - несоответствие типа данных, 2 - данные отсутствуют
    """
    if not isinstance(data, pd.DataFrame):
        print('Тип данных не соответствует pandas.DataFrame!')
        return 1
    elif data is None:
        print('Отсутствуют данные для загрузки в файл "{}"!'.format(filename))
        return 2
    else:
        data.to_csv(filename, sep='\t', encoding='utf-8', index=False)
        # data.to_excel(filename, sheet_name='Sheet1', index=False)
        print('Данные загружены в файл "{}"'.format(filename))
        return 0


def replace_list_by_dict(my_list, my_dict):
    """
    Замена элементов списка значениями из словаря
    :param my_list:
    :param my_dict:
    :return: список со значениями из словаря
    """
    for idx, val in enumerate(my_list):
        if val in list(my_dict.keys()):
            my_list[idx] = my_dict[val]
        else:
            print('Err!')
    return my_list


def interval_extract(arr_list):
    """
    Преобразование списка в интервалы
    :param arr_list: список действительных чисел
    :return: генератор интервалов
    """
    arr_list = sorted(set(arr_list))
    range_start = previous_number = arr_list[0]

    for number in arr_list[1:]:
        if number == previous_number + 1:
            previous_number = number
        else:
            yield [range_start, previous_number]
            range_start = previous_number = number
    yield [range_start, previous_number]


def get_string_intervals(arr_list):
    """
    Преобразование списка, например: [-2.5, -3.5, -1.5, -1, 2, 3, 4, 5, 7, 8, 9, 11], в строку с интервалами,
    вида: "[-3.5...-1.5], -1, [2...5], [7...9], 11". Интервал образовывается последовательными значениями отличными
    после сортировки на 1.
    :param arr_list: список действительных чисел
    :return: строка с интервалами и отдельными значениями через ', '
    """
    intervals = list(interval_extract(arr_list))
    res = ''
    for i, interval in enumerate(intervals):
        if interval[0] == interval[-1]:
            interval = interval[0]
        res += ('', ', ')[1 if i > 0 else 0] + str(interval).replace(', ', '...')
    return res
