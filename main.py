import os
from json_proc import Datamart, get_datamarts_list
from db_mirror import PostgresWrapper
from other import write_csv

# path = r'c:/Users/admin/PycharmProjects/json_parser/data/'
path = r'd:/Git/Python/json_parser/data/'
datamarts_path = path  #
datamarts_file = r'datamarts.json'
metadata_path = path
data_path = path
db_url = 'postgresql://postgres:zahar@localhost:5432/mirror'


def main():

    # Получение списка наименований витрин:
    dm_names_list = get_datamarts_list(os.path.join(datamarts_path, datamarts_file))
    print(os.path.join(datamarts_path, datamarts_file))
    print('-'*50, 'Наименования витрин:', dm_names_list, sep='\n')

    pw = PostgresWrapper(db_url)
    pw.connect()

    for dm_name in dm_names_list:
        dm_name = dm_name.split('/')[-1]  # последняя часть наименования, после разделения по '/'
        # Экземпляр класса витрины:
        dm = Datamart(metadata_path, data_path, dm_name)
        dm_metadata = dm.get_metadata_as_df()
        dm_data = dm.get_data_as_df()

        # Запись таблиц метаданных и данных в отдельные CSV-файлы
        # if dm_name == 'AIRSHINYD00':
        #     write_csv('metadata_{}.csv'.format(dm_name), dm_metadata)
        #     write_csv('data_{}.csv'.format(dm_name), dm_data)
        #     print('-'*50, 'CSV-файлы созданы!', sep='\n')

        # Создание таблицы витрины (с заменой!!!)  в БД:
        pw.create_table_from_df(dm_name, dm_metadata, dm_data)

    pw.close()


if __name__ == "__main__":
    main()
