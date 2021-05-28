"""
db_mirror.py
Модуль для работы с БД PostgreSQL
"""

# (venv)>pip install SQLAlchemy
# (venv)>pip install psycopg2
import logging.config

import sqlalchemy
# from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, String, Date, delete
from sqlalchemy.schema import PrimaryKeyConstraint

types_dict = {
    'C': String,  # CHAR, CUKY
    'N': Integer,  # NUMC
    'P': Float,  # CURR
    'D': Date  # DATS
}

logger = logging.getLogger('app_logger')


class PostgresWrapper:
    """
    Класс для работы с готовой (существующей) БД PostgreSQL
    !!!!!!!!!!!!!!!!!!!!! ВНИМАНИЕ !!!!!!!!!!!!!!!!!!!!!
    rebuild_db=True - пересоздание БД (таблицы пересоздаются, данные удаляются)!
    """

    def __init__(self, host, port, user, password, db_name, rebuild_db=False):
        # self.url = url
        self.host = host
        self.port = port

        self.user = user
        self.password = password
        self.db_name = db_name

        self.rebuild_db = rebuild_db  # признак пересоздания БД
        self.connection = self.connect()
        self.engine = self.connection.engine
        # session = sessionmaker(
        #     bind=self.connection.engine,
        #     autocommit=True,  # все изменения в подключении принимаются
        #     autoflush=True,
        #     enable_baked_queries=False,
        #     expire_on_commit=True  # принимаю изменения от нескольких сессий
        # )
        #
        # self.session = session

    def get_connection(self, db_created=False):
        """
        Создание подключения к БД
        @param:
        """
        engine = create_engine(
            'postgresql://{0}:{1}@{2}:{3}/{4}'.format(
                self.user,
                self.password,
                self.host,
                self.port,
                self.db_name if db_created else ""),
            encoding='utf-8')  # , echo=True)
        # self.connection = self.engine.connect()
        return engine.connect()

    def connect(self):
        connection = self.get_connection()
        if self.rebuild_db:  # пересоздание БД
            connection.execute('DROP DATABASE IF EXISTS {}'.format(self.db_name))
            connection.execute('CREATE DATABASE {}'.format(self.db_name))
        return self.get_connection(db_created=True)

    def delete_table(self, table):
        try:
            table.drop(self.engine)
            logger.debug('Таблица "{0}" удалена!'.format(table.name), sep='\n')
        except Exception as ex:
            logger.debug('Исключение при удалении таблицы "{0}": {1}'.format(table.name, ex), sep='\n')

    def create_table_from_df(self, table_name, df_metadata, df_data):
        try:
            if not self.engine.has_table(table_name):
                # self.delete_table(table)
                logger.debug(logger.debug('Таблица "{0}" не найдена в: "{1}"!'.format(
                    table_name,
                    self.engine)))

                if not table_name == 'AIRFINAN00':
                    # AIRSHINYD00 - data oracle
                    # AIRFINAN00 - double oracle
                    return

                columns_names = list(df_metadata['FIELDNAME'])
                columns_types = list(df_metadata['INTTYPE'])
                for idx, val in enumerate(columns_types):
                    if val in list(types_dict.keys()):
                        columns_types[idx] = types_dict[val]
                    else:
                        logger.error(
                            'При создании таблицы "{0}" обнаружен неизвестный тип данных: "{1}"!'.format(table_name, val),
                            'Таблица не создана!', sep='\n')
                        return
                # primary_key_flags = [True, False, False, False]
                # nullable_flags = [False, False, False, False]
                metadata = MetaData(bind=self.engine)
                table = Table(table_name,
                              metadata,
                              *(Column(column_name, column_type)
                                for column_name, column_type in zip(columns_names, columns_types)
                                ),
                              PrimaryKeyConstraint(*(Column(column_name) for column_name in columns_names),
                                                   name=table_name + '_pk')
                              )

                metadata.create_all(self.engine)  # table.create()
                logger.debug('Таблица "{0}" создана!'.format(table_name))

                df_data.to_sql(name=table_name,
                               con=self.connection,
                               if_exists='append',
                               index=False
                               )
            else:
                logger.debug('Таблица "{0}" уже существует в: "{1}"!'.format(table_name, self.engine))
            # metadata = MetaData(bind=self.connection)
            # table = Table(table_name,
            #               metadata,
            #               *(Column(column_name, column_type)
            #                 for column_name, column_type in zip(columns_names, columns_types)
            #                 ),
            #               PrimaryKeyConstraint(*(Column(column_name) for column_name in columns_names),
            #                                    name=table_name + '_pk')
            #               )

            # if self.engine. has_table(table_name):
            #     logger.debug('Таблица "{0}" уже существует в: "{1}"!'.format(table_name, self.engine))
            #     # self.delete_table(table)

            # table.create()
            # logger.debug('Таблица "{0}" создана!'.format(table_name))
            #
            # df_data.to_sql(name=table_name,
            #                con=self.connection,
            #                if_exists='append',
            #                index=False
            #                )

        except Exception as ex:
            logger.error('Исключение при создании таблицы "{0}": {1}'.format(table_name, ex))

    def close(self):
        if self.connection is not None:
            self.connection.close()
