import os
import argparse
from log_settings import logger_config
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

    # Подключение к БД:
    # db_url_example = 'postgresql://postgres:zahar@localhost:5432/mirror'
    pw = PostgresWrapper(
        config['host'],
        config['port'],
        config['username'],
        config['password'],
        config['database'])
    print(pw)

    for dm_name in dm_names_list:
        dm_name = dm_name.split('/')[-1]  # последняя часть наименования, после разделения по '/'
        # Экземпляр класса витрины:
        # Запись таблиц метаданных и данных в отдельные CSV-файлы
        if dm_name == 'AIRSHINYD00':
            # write_csv('metadata_{}.csv'.format(dm_name), dm_metadata)
            # write_csv('data_{}.csv'.format(dm_name), dm_data)
            # print('-'*50, 'CSV-файлы созданы!', sep='\n')

            dm = Datamart(metadata_path, data_path, dm_name)

            # Создание таблицы витрины (с заменой!!!)  в БД:
            pw.create_table_from_df(dm_name, dm.metadata, dm.data)

    pw.close()


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