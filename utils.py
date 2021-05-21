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

