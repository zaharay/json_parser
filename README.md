# json_parser - проект создания ETL-инструмента (программы) для загрузки, преобразования и выгрузки данных информационных витрин.<br>
**Источник данных:** web-сервисы метаданных и данных витрин<br>
**Преобразование:** парсинг JSON, проверки и преобразования типов данных, импорт в Pandas DataFrame<br>
**Получатель данных:** БД PostgreSQL<br>
**Формат источника данных:** JSON

## Перечень файлов проекта:
*   config.txt - файл конфигурации
*   debug.log - журнал (создается и наполняется в процессе работы программы)
*   main.py - главный файл программы
*   settings.py - модуль настроек логгеров
*   json_proc.py - модуль получения параметров конфигурации (ссылок на ресурсы витрин) и базового класса витрины
*   db_mirror.py - модуль для работы с БД PostgreSQL
*   utils.py - модуль со вспомогательными инструментами (функциями/методами):

## Полезные ссылки (по разделам):
*   [REST API, Requests, JSON response](#requests)
*   [SQL, SQLAlchemy](#db)
*   [Версия интерпретатора Python и перечень используемых модулей](#python)

#### REST API, Requests, JSON response:<a name="request"></a>
*   [REST API: что это простыми словами: принципы, стандарты, описание](https://boodet.online/reastapi)
*   [Requests в Python – Примеры выполнения HTTP запросов](https://python-scripts.com/requests)
*   [How to write a simple Postgres JSON wrapper with Python](https://levelup.gitconnected.com/how-to-write-a-simple-postgres-json-wrapper-with-python-ef09572daa66)
*   [How do I get JSON data from RESTful service using Python?](https://stackoverflow.com/questions/7750557/how-do-i-get-json-data-from-restful-service-using-python)
*   [Read JSON response in Python](https://stackoverflow.com/questions/33282067/read-json-response-in-python)

#### SQL, SQLAlchemy:<a name="db"></a>
*   [Создание схемы базы данных в SQLAlchemy Core](https://pythonru.com/biblioteki/shemy-sqlalchemy-core)
*   

#### Версия интерпретатора Python и перечень используемых модулей:<a name="python"></a>
*   Python - Python 3.5.3
*   lxml - v. 4.6.3
*   pytz - v. 2021.1
*   numpy - v. 1.18.5
*   python-dateutil - v. 2.8.1
*   six - v. 1.15.0
*   pandas - v. 0.25.3
*   chardet - v. 4.0.0
*   urllib3 - v. 1.26.4
*   idna - v. 2.10
*   certifi - v. 2020.12.5
*   requests - v. 2.25.1 
*   psycopg2 - v. 2.8.6
*   SQLAlcheny - v. 1.3.24
*   
