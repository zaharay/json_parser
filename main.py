import os
import argparse
from settings import logger_config
from utils import config_parser
import logging.config
from json_proc import Datamart, get_datamarts_list
from db_mirror import PostgresWrapper


# Локализация (константа):
LOCATION = 'work_localhost'  # 'host'  # 'home_localhost'

logging.config.dictConfig(logger_config)
s_logger = logging.getLogger('simple_logger')
logger = logging.getLogger('app_logger')


def main():
    s_logger.debug('Cтарт')

    datamarts_path = ''
    metadata_path = ''
    data_path = ''
    config = config_parser(args.config, section=LOCATION)
    if LOCATION is 'work_localhost':
        datamarts_file = r'datamarts.json'
        datamarts_path = config['path']
        datamarts_path = os.path.join(datamarts_path, datamarts_file)


    db_url = 'postgresql://postgres:zahar@localhost:5432/mirror'

    # Получение списка наименований витрин:
    dm_names_list = get_datamarts_list(datamarts_path)
    print('-'*50, 'Наименования витрин:', dm_names_list, sep='\n')

    # pw = PostgresWrapper(db_url)
    # pw.connect()
    # print(pw)
    # for dm_name in dm_names_list:
    #     dm_name = dm_name.split('/')[-1]  # последняя часть наименования, после разделения по '/'
    #     # Экземпляр класса витрины:
    #     dm = Datamart(metadata_path, data_path, dm_name)
    #     dm_metadata = dm.get_metadata_as_df()
    #     dm_data = dm.get_data_as_df()
    #
    #     # Запись таблиц метаданных и данных в отдельные CSV-файлы
    #     # if dm_name == 'AIRSHINYD00':
    #     #     write_csv('metadata_{}.csv'.format(dm_name), dm_metadata)
    #     #     write_csv('data_{}.csv'.format(dm_name), dm_data)
    #     #     print('-'*50, 'CSV-файлы созданы!', sep='\n')
    #
    #     # Создание таблицы витрины (с заменой!!!)  в БД:
    #     pw.create_table_from_df(dm_name, dm_metadata, dm_data)
    #
    # pw.close()
    s_logger.debug('Cтоп')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--config',
                        type=str,
                        help='path to configuration file',
                        default='./config.txt',  # os.path.realpath('config.txt'),
                        dest='config'
                        )
    args = parser.parse_args()

    main()
