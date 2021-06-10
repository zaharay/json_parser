
import argparse
from datetime import datetime
from log_settings import logger_config
from utils import config_parser, get_timedelta
from json_proc import *
from db_mirror import PostgresWrapper


# Локализация (константа):
LOCATION = 'work_localhost'  # 'work_localhost' 'host' 'home_localhost'

logging.config.dictConfig(logger_config)
s_logger = logging.getLogger('simple_logger')  # логгер для старта и стопа
logger = logging.getLogger('app_logger')  # основной логгер


def main():

    config = config_parser(args.config, section=LOCATION)
    if config is None:
        return -1

    data_dict = get_params(config, LOCATION)
    if data_dict is None:
        return -1

    if LOCATION == 'host':
        logger.debug('\nКонфигурация:\n\tdatamarts_path = {0}\n\tmetadata_path = {1}\n\tdata_path = {2}'.format(
            data_dict['datamarts_path'],
            data_dict['metadata_path'],
            data_dict['data_path']))
    else:  # 'work_localhost' or 'home_localhost'
        logger.debug('\nКонфигурация:\n\tpath = {0}\n\tfile = {1}'.format(
            data_dict['path'], data_dict['datamarts_file']))

    logger.debug('\nНаименования обрабатываемых витрин:\n\t{}'.format(data_dict['datamarts_list']))

    # Чтение метаданных витрин:
    metadata_dict = {}  # словарь метаданных витрин
    for dm_name in data_dict['datamarts_list']:
        key = dm_name.split('/')[-1]  # последняя часть наименования, после разделения по '/'
        if LOCATION == 'host':
            path = data_dict['metadata_path'] + r'?tabname=' + dm_name
        else:  # 'work_localhost' or 'home_localhost'
            path = data_dict['path'] + '/metadata_{}.json'.format(key)
        metadata_dict[key] = get_df_from_json(path, 'METADATA', LOCATION)
        if metadata_dict[key] is None:
            return -1

    # Подключение к БД:
    # db_url_example = 'postgresql://postgres:zahar@localhost:5432/mirror'
    pw = PostgresWrapper(
        config['host'],
        config['port'],
        config['username'],
        config['password'],
        config['database'])
    if pw.connection is None:
        return -1

    # Чтение данных, создание и наполнение таблиц в БД:
    for dm_name in data_dict['datamarts_list']:
        key = dm_name.split('/')[-1]  # последняя часть наименования, после разделения по '/'
        if LOCATION == 'host':
            path = data_dict['data_path'] + r'?tabname=' + dm_name + \
                   r'&max_rows=' + str(data_dict['max_rows']) + r'&format=' + str(data_dict['data_format'])
        else:  # 'work_localhost' or 'home_localhost'
            path = data_dict['path'] + '/data_{}.json'.format(key)
        dm_data = get_df_from_json(path, 'DATA', LOCATION)
        if dm_data is None:
            return -1

        if pw.fast_create_table_from_df(dm_name, metadata_dict[key], dm_data) is None:
            return -1

    pw.close()


if __name__ == "__main__":
    s_logger.debug('Cтарт')
    str_start = datetime.now().strftime('%d-%m-%Y %H:%M:%S')

    parser = argparse.ArgumentParser()
    parser.add_argument('--config',
                        type=str,
                        help='path to configuration file',
                        default='./config.txt',  # os.path.realpath('config.txt'),
                        dest='config'
                        )
    args = parser.parse_args()

    main()

    logger.debug('\nВремя работы скрипта: {}'.format(get_timedelta(str_start,
                                                                   datetime.now().strftime('%d-%m-%Y %H:%M:%S'))))
    s_logger.debug('Cтоп')