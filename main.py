import os
from json_proc import Datamart, get_datamarts_list
from db_mirror import PostgresWrapper

# path = 'c:/Users/admin/PycharmProjects/json_parser/data/'
# path = 'd:/Git/Python/json_parser/data'
datamarts_path = r'c:/Users/admin/PycharmProjects/json_parser/data/'
datamarts_file = r'datamarts.json'
metadata_path = r'c:/Users/admin/PycharmProjects/json_parser/data/'
data_path = r'c:/Users/admin/PycharmProjects/json_parser/data/'
db_url = 'postgresql://postgres:zahar@localhost:5432/mirror'

def write_csv(filename, data):
    """Запись данных в CSV-файл"""
    if data is not None:
        data.to_csv(filename, sep='\t', encoding='utf-8', index=False)
        # data.to_excel(filename, sheet_name='Sheet1', index=False)
        print('Данные загружены в файл "{}"'.format(filename))
    else:
        print('Отсутствуют данные для загрузки в файл {}!'.format(filename))

def main():
    replace(li, data_types)
    print(li)

    # Получение списка витрин:
    datamarts_list = get_datamarts_list(os.path.join(datamarts_path, datamarts_file))
    print('-'*50, 'Список витрин:', datamarts_list, sep='\n')

    dm_name = datamarts_list[0].split('/')[-1]

    # Экземпляр витрины
    dm1 = Datamart(metadata_path, data_path, dm_name)
    dm1_metadata = dm1.get_metadata_as_df()
    dm1_data = dm1.get_data_as_df()

    # write_csv('metadata_{}.csv'.format(dm1.name), dm1_metadata)
    # write_csv('data_{}.csv'.format(dm1.name), dm1_data)
    #
    # pw = PostgresWrapper(db_url)
    # pw.create_table_from_metadata_df(dm_name, dm1_metadata)
    # pw.connect()
    # print(pw.connection)



if __name__ == "__main__":
    main()
