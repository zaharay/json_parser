# (venv)>pip install SQLAlchemy
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, String, DateTime

types_dict = {
    'C': String,    # CHAR, CUKY
    'N': Integer,   # NUMC
    'P': Float,     # CURR
}


def replace_list_by_dict(my_list, my_dict):
    for idx, val in enumerate(my_list):
        if val in list(my_dict.keys()):
            my_list[idx] = my_dict[val]
        else:
            print('Err!')
    return my_list


class PostgresWrapper:
    def __init__(self, url):
        self.url = url
        self.connection = None

    def connect(self):
        engine = create_engine(self.url, echo=True)
        self.connection = engine.connect()

    def create_table_from_metadata_df(self, name, metadata):
        try:
            columns_names = list(metadata['FIELDNAME'])
            columns_types = list(metadata['INTTYPE'])
            for idx, val in enumerate(columns_types):
                if val in list(types_dict.keys()):
                    columns_types[idx] = types_dict[val]
                else:
                    print('При создании таблицы "{0}" обнаружен неизвестный тип данных: "{1}"!',
                          'Таблица не создана!', sep='\n')
                    return
            print(columns_types)
            # primary_key_flags = [True, False, False, False]
            # nullable_flags = [False, False, False, False]
            #
            # meta = MetaData()
            # table = Table(name, meta,
            #               *(Column(column_name, column_type,
            #                        primary_key=primary_key_flag,
            #                        nullable=nullable_flag)
            #                 for column_name,
            #                     column_type,
            #                     primary_key_flag,
            #                     nullable_flag in zip(columns_names,
            #                                          columns_types,
            #                                          primary_key_flags,
            #                                          nullable_flags)))

        except Exception as ex:
            print('Исключение при создании таблицы "{0}" в БД: {1}'.format(self.name, ex))
