import os
import argparse
from settings import logger_config
from utils import config_parser
import logging.config
from json_proc import Datamart, get_params
from db_mirror import PostgresWrapper


# Локализация (константа) - источник данных:
LOCATION = 'work_localhost'  # 'work_localhost' 'host' 'home_localhost'

logging.config.dictConfig(logger_config)
s_logger = logging.getLogger('simple_logger')  # логгер для старта и стопа
logger = logging.getLogger('app_logger')  # основной логгер


def main():
    config = config_parser(args.config, section=LOCATION)
    datamarts_path, metadata_path, data_path, dm_names_list = get_params(config, LOCATION)
    logger.debug('\nКонфигурация:\n\tdatamarts_path = {0}\n\tmetadata_path = {1}\n\tdata_path = {2}'.format(
        datamarts_path, metadata_path, data_path))
    logger.debug('\nНаименования витрин:\n\t{}'.format(dm_names_list))

    # Получение URL БД:
    # db_url_example = 'postgresql://postgres:zahar@localhost:5432/mirror'
    db_name = config['database']
    username = config['username']
    password = config['password']
    host = config['host']
    port = config['port']

    # pw = PostgresWrapper(host, port, username, password, db_name)
    # print(pw)

    for dm_name in dm_names_list:
        dm_name = dm_name.split('/')[-1]  # последняя часть наименования, после разделения по '/'
        # Экземпляр класса витрины:
        # Запись таблиц метаданных и данных в отдельные CSV-файлы
        if dm_name == 'AIRFINAN00':
            # write_csv('metadata_{}.csv'.format(dm_name), dm_metadata)
            # write_csv('data_{}.csv'.format(dm_name), dm_data)
            # print('-'*50, 'CSV-файлы созданы!', sep='\n')
            dm = Datamart(metadata_path, data_path, dm_name)
            # dm_metadata = dm.get_metadata_as_df()
            # dm_data = dm.get_data_as_df()





        # Создание таблицы витрины (с заменой!!!)  в БД:
        # pw.create_table_from_df(dm_name, dm_metadata, dm_data)

    # pw.close()


if __name__ == "__main__":
    s_logger.debug('Cтарт')

    parser = argparse.ArgumentParser()
    parser.add_argument('--config',
                        type=str,
                        help='path to configuration file',
                        default='./config.txt',  # os.path.realpath('config.txt'),
                        dest='config'
                        )
    args = parser.parse_args()

    main()

    s_logger.debug('Cтоп')