"""
db_mirror.py
Модуль для работы с БД PostgreSQL
"""

import logging.config
from sqlalchemy import create_engine, inspect, delete
from sqlalchemy import MetaData, Table, Column
from sqlalchemy.sql.ddl import DropConstraint, DropTable
from sqlalchemy.sql.sqltypes import String, SchemaType
from sqlalchemy.schema import PrimaryKeyConstraint, ForeignKeyConstraint

logger = logging.getLogger('app_logger')


class PostgresWrapper:
    """
    Класс для работы с готовой (существующей) БД PostgreSQL
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

    def get_connection(self, db_created=False):
        """
        Создание подключения к БД
        @param:
        """
        try:
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
        except Exception as err:
            logger.exception('\nОшибка подключения к БД:\n\t{}'.format(err))
            return None

    def connect(self):
        connection = self.get_connection()
        if connection is None:
            return None
        if self.rebuild_db:  # пересоздание БД
            connection.execute('DROP DATABASE IF EXISTS {}'.format(self.db_name))
            connection.execute('CREATE DATABASE {}'.format(self.db_name))
        return self.get_connection(db_created=True)

    def drop_table(self, table):
        try:
            table.drop(self.connection.engine)
            logger.debug('Таблица "{0}": Успешно удалена!'.format(table.name), sep='\n')
        except Exception as ex:
            logger.debug('Исключение при удалении таблицы "{0}": {1}'.format(table.name, ex), sep='\n')

    def gentle_drop_table(self, table_name):
        try:
            conn = self.connection  # engine.connect()

            # Транзакция применяется только в том случае, если БД поддерживает транзакционный DDL (язык описанияданных),
            # т.е. в таких: PostgreSQL, MS SQL Server
            trans = conn.begin()
            # inspector = reflection.Inspector.from_engine(engine)
            inspector = inspect(self.connection.engine)
            # Сначала собираю все данные, прежде чем что-либо сбрасывать (drop).
            # Некоторые БД блокируются после того, как что-то было удалено в транзакции.
            metadata = MetaData()

            types = []
            foreign_keys = []

            for f_key in inspector.get_foreign_keys(table_name):
                if not f_key['name']:
                    continue
                foreign_keys.append(
                    ForeignKeyConstraint((), (), name=f_key['name'])
                )

            for col in inspector.get_columns(table_name):
                if isinstance(col['type'], SchemaType):
                    types.append(col['type'])
            table = Table(table_name, metadata, *foreign_keys)

            for f_key in foreign_keys:
                conn.execute(DropConstraint(f_key))
            conn.execute(DropTable(table))

            for custom_type in types:
                custom_type.drop(conn)
            trans.commit()
        except Exception as ex:
            logger.debug('Исключение при "деликатном" удалении: {0}'.format(ex), sep='\n')
            trans.rollback()
            # raise

    def close(self):
        if self.connection is not None:
            self.connection.close()

    def fast_create_table_from_df(self, table_name, df_metadata, df_data):
        """
        Быстрое создание и наполнение таблиц (без проверок соответствия типов данных и структуры)
        """
        try:
            # Флаг создания/пересоздания таблицы:
            # * True - таблица создается/пересоздается в любом случае
            # * False - если создавать/пересоздавать требуется по условиям (см. ниже)
            logger.debug('*' * 10 + ' Таблица "{0}" '.format(table_name) + '*' * 10)

            if self.connection.engine.has_table(table_name):
                logger.debug('Таблица уже существует в: "{}"!'.format(self.connection.engine))

                # Удаление таблицы:
                self.gentle_drop_table(table_name)
                logger.debug('Успешно удалена!')
            else:
                logger.debug('Не найдена в: "{}"!'.format(self.connection.engine))

            columns_types = {}
            for index, row in df_metadata.iterrows():
                columns_types[row['FIELDNAME']] = String  # все столбцы строковые

            metadata = MetaData(bind=self.connection.engine)
            table = Table(table_name,
                          metadata,
                          *(Column(column_name, column_type)
                            for column_name, column_type in zip(columns_types.keys(), columns_types.values())
                            ),
                          PrimaryKeyConstraint(*(Column(column_name) for column_name in df_metadata['FIELDNAME']),
                                               name=table_name + '_pk')
                          )

            metadata.create_all(self.connection.engine)  # table.create()

            df_data.to_sql(name=table_name,
                           con=self.connection,
                           if_exists='append',
                           index=False
                           )
            logger.debug('Успешно создана!')
            return 0

        except Exception as ex:
            logger.error('\nИсключение при создании таблицы: {}'.format(ex))
            return None
