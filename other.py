import pandas as pd

def write_csv(filename, data):
    """Запись данных в CSV-файл"""
    if data is not None:
        data.to_csv(filename, sep='\t', encoding='utf-8', index=False)
        # data.to_excel(filename, sheet_name='Sheet1', index=False)
        print('Данные загружены в файл "{}"'.format(filename))
    else:
        print('Отсутствуют данные для загрузки в файл {}!'.format(filename))

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
