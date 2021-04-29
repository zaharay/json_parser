import os
from json_proc import Datamart, get_datamarts_list

# path = 'c:/Users/admin/PycharmProjects/json_parser/data/'
# path = 'd:/Git/Python/json_parser/data'
datamarts_path = r'c:/Users/admin/PycharmProjects/json_parser/data/'
datamarts_file = r'datamarts.json'
metadata_path = r'c:/Users/admin/PycharmProjects/json_parser/data/'
data_path = r'c:/Users/admin/PycharmProjects/json_parser/data/'


def write_csv(filename, data):
    """Запись данных в CSV-файл"""
    if data is not None:
        data.to_csv(filename, sep='\t', encoding='utf-8', index=False)
        # data.to_excel(filename, sheet_name='Sheet1', index=False)
        print('Данные загружены в файл "{}"'.format(filename))
    else:
        print('Отсутствуют данные для загрузки в файл {}!'.format(filename))


def main():
    # Получение списка витрин:
    datamarts_list = get_datamarts_list(os.path.join(datamarts_path, datamarts_file))
    print('-'*50, 'Список витрин:', datamarts_list, sep='\n')

    dm1 = Datamart(metadata_path, data_path, 'AIRSHINYL00')
    dm1_metadata = dm1.get_metadata_as_df()
    dm1_data = dm1.get_data_as_df()

    # write_csv('metadata_{}.csv'.format(dm1.name), dm1_metadata)
    # write_csv('data_{}.csv'.format(dm1.name), dm1_data)


if __name__ == "__main__":
    main()
