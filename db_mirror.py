"""
db_mirror.py
Модуль для работы с БД PostgreSQL
"""

# (venv)>pip install SQLAlchemy
# (venv)>pip install psycopg2
import logging.config
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, String, DateTime, delete

types_dict = {
    'C': String,    # CHAR, CUKY
    'N': Integer,   # NUMC
    'P': Float,     # CURR
    'D': DateTime   # DATS
}

logger = logging.getLogger('app_logger')

class PostgresWrapper:
    """
    Класс для работы с готовой (существующей) БД PostgreSQL

    !!!!!!!!!!!!!!!!!!!!! ВНИМАНИЕ !!!!!!!!!!!!!!!!!!!!!
    Существующие в БД таблицы пересоздаются (данные удаляются)!
    """
    def __init__(self, url):
        self.url = url
        self.engine = None
        self.connection = None

    def connect(self):
        self.engine = create_engine(self.url)  #, echo=True)
        self.connection = self.engine.connect()

    def delete_table(self, table):
        try:
            table.drop(self.engine)
            logger.debug('Таблица "{0}" удалена!'.format(table.name), sep='\n')
        except Exception as ex:
            logger.debug('Исключение при удалении таблицы "{0}": {1}'.format(table.name, ex), sep='\n')

    def create_table_from_df(self, table_name, df_metadata, df_data):
        try:
            columns_names = list(df_metadata['FIELDNAME'])
            columns_types = list(df_metadata['INTTYPE'])
            for idx, val in enumerate(columns_types):
                if val in list(types_dict.keys()):
                    columns_types[idx] = types_dict[val]
                else:
                    logger.debug(
                          'При создании таблицы "{0}" обнаружен неизвестный тип данных: "{1}"!'.format(table_name, val),
                          'Таблица не создана!', sep='\n')
                    return
            # primary_key_flags = [True, False, False, False]
            # nullable_flags = [False, False, False, False]

            metadata = MetaData(bind=self.connection)
            table = Table(table_name,
                          metadata,
                          *(Column(column_name, column_type)
                            for column_name, column_type in zip(columns_names, columns_types)
                            ),
                          )

            if self.engine.has_table(table_name):
                logger.debug('Таблица "{0}" уже существует в: "{1}"!'.format(table_name, self.engine), sep='\n')
                self.delete_table(table)

            table.create()
            logger.debug('Таблица "{0}" создана!'.format(table_name), sep='\n')

            df_data.to_sql(name=table_name,
                           con=self.connection,
                           if_exists='append',
                           index=False
                           )

        except Exception as ex:
            logger.debug('Исключение при создании таблицы "{0}": {1}'.format(table_name, ex), sep='\n')

    def close(self):
        if self.connection is not None:
            self.connection.close()
