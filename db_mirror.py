"""
db_mirror.py
Модуль для работы с БД PostgreSQL
"""

# (venv)>pip install SQLAlchemy
# (venv)>pip install psycopg2
import logging.config
import datetime
from sqlalchemy import create_engine, inspect, delete
from sqlalchemy import MetaData, Table, Column
from sqlalchemy.sql.sqltypes import Integer, Float, String, Date
from sqlalchemy.dialects.postgresql import VARCHAR, INTEGER, DATE, DOUBLE_PRECISION
from sqlalchemy.schema import PrimaryKeyConstraint


logger = logging.getLogger('app_logger')

_type_bw2alchemy = {
    'C': String,   # CHAR, CUKY
    'N': Integer,  # NUMC
    'P': Float,    # CURR
    'D': Date      # DATS
}

_type_alchemy2sql = {
    String: 'VARCHAR',
    Integer: 'INTEGER',
    Float: 'DOUBLE_PRECISION',
    Date: 'DATE'
}

_type_sql2alchemy = {
    'VARCHAR': String,
    'INTEGER': Integer,
    'DOUBLE_PRECISION': Float,
    'DATE': Date
}

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
            # new_columns_names = list(df_metadata['FIELDNAME'])
            # new_columns_types = list(df_metadata['INTTYPE'])
            create_flag = False  # флаг создания таблицы

            new_columns = {}
            for index, row in df_metadata.iterrows():
                bw_type = row['INTTYPE']
                if bw_type in list(_type_bw2alchemy.keys()):
                    new_columns[row['FIELDNAME']] = _type_bw2alchemy[bw_type]  # приведение типа BW к типу SQLAlchemy
                else:
                    logger.error(
                        'При создании таблицы "{0}" обнаружен неизвестный тип данных: "{1}"!'.format(table_name, val),
                        'Таблица не создана!', sep='\n')
                    return
            # print(*zip(new_columns.keys(), new_columns.values()), sep='\n')

            # for idx, val in enumerate(new_columns_types):
            #     if val in list(_type_bw2alchemy.keys()):
            #         new_columns_types[idx] = _type_bw2alchemy[val]
            #     else:
            #         logger.error(
            #             'При создании таблицы "{0}" обнаружен неизвестный тип данных: "{1}"!'.format(table_name, val),
            #             'Таблица не создана!', sep='\n')
            #         return

            if self.engine.has_table(table_name):
                logger.debug('Таблица "{0}" уже существует в: "{1}"!'.format(table_name, self.engine))
                # meta = MetaData()
                # meta.bind = self.engine
                # meta.reflect()
                # datatable = meta.tables[table_name]
                # print([str(c.type) for c in datatable.columns])
                # return

                inspector = inspect(self.engine)
                exist_columns = inspector.get_columns(table_name)

                # Проверка наличия столбца в таблице (по наименованию):
                for new_col_name in new_columns.keys():
                    if new_col_name not in [str(col['name']) for col in exist_columns]:
                        logger.debug('В таблице "{0}" отсутствует столбец с наименованием: "{1}"!'.format(
                            table_name, new_col_name))
                        create_flag = True  # требуется пересоздание таблицы
                        break

                    # Проверка типа данных столбца:
                    exist_col_type = str([exist_col['type'] for exist_col in exist_columns if exist_col['name'] ==
                                          new_col_name][0])
                    exist_col_type = _type_sql2alchemy[exist_col_type].__name__
                    new_col_type = new_columns[new_col_name].__name__

                    if not  new_col_type == exist_col_type:
                        logger.debug(f'В таблице "{table_name}" тип столбца "{new_col_name}" ({new_col_type})'
                                     f' не соответствует существующему ({exist_col_type})!')
                        create_flag = True  # требуется пересоздание таблицы
                        break

                logger.debug((f'Структура загружаемых данных таблицы "{table_name}" соответствует существующей',
                              f'Пересоздание таблицы "{table_name}"!')[1 if create_flag else 0])

                return
                # exist_columns_names =
                print(exist_columns_names)
                return

                for column in inspector.get_columns(table_name):
                    print(column['name'], column['type'])
                # print(inspector.get_columns(table_name)['name'])
                print(columns_names)
                print(*columns_types, sep='\n')

                # self.delete_table(table)

                return

            logger.debug(logger.debug('Таблица "{0}" не найдена в: "{1}"!'.format(
                table_name,
                self.engine)))

            # if not table_name == 'AIRFINAN00':
            #     # AIRSHINYD00 - data oracle
            #     # AIRFINAN00 - double oracle
            #     return

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


                # for schema in schemas:
                #     print("schema: %s" % schema)
                #     for column in inspector.get_columns(table_name, schema=schema):
                #         print("Column: %s" % column)
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
