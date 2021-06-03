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
from sqlalchemy.sql.ddl import DropConstraint, DropTable
from sqlalchemy.sql.sqltypes import Integer, Float, String, Date, SchemaType
from sqlalchemy.dialects.postgresql import VARCHAR, INTEGER, DATE, DOUBLE_PRECISION
from sqlalchemy.schema import PrimaryKeyConstraint, ForeignKeyConstraint


logger = logging.getLogger('app_logger')

# Словарь соответствия типов данных при конвертации из DataFrame в SQLAlchemy:
_type_bw2alchemy = {
    'C': String,   # CHAR, CUKY
    'N': Integer,  # NUMC
    'P': Float,    # CURR
    'D': String    # Не 'Date' потому, что форматы даты (допустимые значения) в BW и Postgres отличаются!   # DATS
}

# Словарь соответствия типов данных для проверки соответствия существующих в DB c посупившими из DataFrame:
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

    def drop_table(self, table):
        try:
            table.drop(self.engine)
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
            inspector = inspect(self.engine)
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

    def create_table_from_df(self, table_name, df_metadata, df_data):
        try:
            # Флаг создания/пересоздания таблицы:
            # * True - таблица создается/пересоздается в любом случае
            # * False - если создавать/пересоздавать требуется по условиям (см. ниже)
            logger.debug('*'*10 + ' Таблица "{0}" '.format(table_name) + '*'*10)

            create_flag = False

            new_columns = {}
            for index, row in df_metadata.iterrows():
                bw_type = row['INTTYPE']
                if bw_type in list(_type_bw2alchemy.keys()):
                    new_columns[row['FIELDNAME']] = _type_bw2alchemy[bw_type]  # приведение типа BW к типу SQLAlchemy
                else:
                    logger.error(
                        'Обнаружен неизвестный тип данных: "{}"!'.format(bw_type),
                        'Таблица не создана!', sep='\n')
                    return
            # print(*zip(new_columns.keys(), new_columns.values()), sep='\n')

            if self.engine.has_table(table_name):
                logger.debug('Таблица уже существует в: "{}"!'.format(self.engine))

                inspector = inspect(self.engine)
                exist_columns = inspector.get_columns(table_name)

                # Проверка наличия столбца в таблице (по наименованию):
                for new_col_name in new_columns.keys():
                    if new_col_name not in [str(col['name']) for col in exist_columns]:
                        logger.debug('В таблице отсутствует столбец с наименованием: "{}"!'.format(new_col_name))
                        create_flag = True  # требуется пересоздание таблицы
                        break

                    # Проверка типа данных столбца:
                    exist_col_type = str([exist_col['type'] for exist_col in exist_columns if exist_col['name'] ==
                                          new_col_name][0])
                    exist_col_type = _type_sql2alchemy[exist_col_type].__name__
                    new_col_type = new_columns[new_col_name].__name__

                    if not new_col_type == exist_col_type:
                        logger.debug('Тип столбца "{0}" ({1}) не соответствует '
                                     'существующему ({2})!'.format(new_col_name,
                                                                   new_col_type,
                                                                   exist_col_type))

                        create_flag = True  # требуется пересоздание таблицы
                        break

                logger.debug(('Структура загружаемых данных соответствует существующей!',
                              'Структура загружаемых данных отличается от существующей!')[1 if create_flag else 0])

                # Удаление таблицы:
                # if create_flag:  # раскомментир-ть, если таблица создается/пересоздается по условиям (см. выше)
                self.gentle_drop_table(table_name)
                logger.debug('Успешно удалена!')
            else:
                logger.debug('Не найдена в: "{}"!'.format(self.engine))
                create_flag = True  # требуется создание таблицы

            # if create_flag:  # раскомментир-ть, если таблица создается/пересоздается по условиям (см. выше)

            # primary_key_flags = [True, False, False, False]
            # nullable_flags = [False, False, False, False]
            metadata = MetaData(bind=self.engine)
            table = Table(table_name,
                          metadata,
                          *(Column(column_name, column_type)
                            for column_name, column_type in zip(new_columns.keys(), new_columns.values())
                            ),
                          PrimaryKeyConstraint(*(Column(column_name) for column_name in new_columns.keys()),
                                               name=table_name + '_pk')
                          )

            metadata.create_all(self.engine)  # table.create()

            df_data.to_sql(name=table_name,
                           con=self.connection,
                           if_exists='append',
                           index=False
                           )
            logger.debug('Успешно создана!')

        except Exception as ex:
            logger.error('Исключение при создании таблицы: {}'.format(ex))

    def close(self):
        if self.connection is not None:
            self.connection.close()
