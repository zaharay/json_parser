"""
settings.py
Модуль настроек логгеров:
simple_logger - логгер для сообщений 'Старт' и 'Стоп'
app_logger - логгер для сообщений с приоритетом от DEBUG и выше
"""

import logging


class MegaHandler(logging.Handler):
    def __init__(self, filename):
        logging.Handler.__init__(self)
        self.filename = filename

    def emit(self, record):
        message = self.format(record)
        with open(self.filename, 'a', encoding='utf-8') as file:
            file.write(message + '\n')


logger_config = {
    'version': 1,
    'disable_existing_loggers': False, # отключение остальных логгеров

    'formatters': {  # форматировщики
        'simple_format': {  # при старте и остановке программы
            'format': '-------------------- {message}: {asctime} --------------------',
            'style': '{',
            'datefmt': '%d-%m-%Y %H:%M:%S'
        },
        'std_format': {  # для остальных сообщений
            # 'format': '{asctime}.{msecs:0<3.0f} - {levelname} - {name} - {module}:{funcName}:{lineno} - {message}',
            'format': '{asctime}.{msecs:0<3.0f} - {levelname} - {module}:{funcName}:{lineno} - {message}',
            'style': '{',
            'datefmt': '%d-%m-%Y %H:%M:%S'
        }
    },
    'handlers': {  # обработчики
        'simple_console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'simple_format'
        },
        'simple_file': {
            '()': MegaHandler,  # экземпляр MegaHandler
            'level': 'DEBUG',
            'filename': 'debug.log',
            'formatter': 'simple_format'
        },
        'std_console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'std_format'
        },
        'std_file': {
            '()': MegaHandler,  # экземпляр MegaHandler
            'level': 'DEBUG',
            'filename': 'debug.log',
            'formatter': 'std_format'
        }
    },
    'loggers': {  # логгеры
        'app_logger': {
            'level': 'DEBUG',
            'handlers': ['std_console', 'std_file']
            #'propagate': False
        },
        'simple_logger': {
            'level': 'DEBUG',
            'handlers': ['simple_console', 'simple_file']
            #'propagate': False
        }
    },

    # 'filters': {},
    # 'root': {}   # '': {}
    # 'incremental': True
}
