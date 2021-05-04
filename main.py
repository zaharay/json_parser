import os
from json_proc import Datamart, get_datamarts_list
from db_mirror import PostgresWrapper

# path = r'c:/Users/admin/PycharmProjects/json_parser/data/'
path = r'd:/Git/Python/json_parser/data/'
datamarts_path = path  #
datamarts_file = r'datamarts.json'
metadata_path = path
data_path = path
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

    # Получение списка витрин:
    datamarts_list = get_datamarts_list(os.path.join(datamarts_path, datamarts_file))
    print(os.path.join(datamarts_path, datamarts_file))
    print('-'*50, 'Список витрин:', datamarts_list, sep='\n')

    dm_name = datamarts_list[0].split('/')[-1]  # последняя часть наименования, после разделения по '/'

    # Экземпляр витрины
    dm1 = Datamart(metadata_path, data_path, dm_name)
    dm1_metadata = dm1.get_metadata_as_df()
    dm1_data = dm1.get_data_as_df()

    # write_csv('metadata_{}.csv'.format(dm1.name), dm1_metadata)
    # write_csv('data_{}.csv'.format(dm1.name), dm1_data)

    pw = PostgresWrapper(db_url)
    pw.connect()
    pw.create_table_from_df(dm_name, dm1_metadata, dm1_data)
    pw.close()

if __name__ == "__main__":
    main()
