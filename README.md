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
*   [Object Relational Mapping (ORM)](#orm)
*   [Версия интерпретатора Python и перечень используемых модулей](#python)
*	[Ansible/AWX](#ansible)

#### REST API, Requests, JSON response:<a name="request"></a>
*   [REST API: что это простыми словами: принципы, стандарты, описание](https://boodet.online/reastapi)
*   [Requests в Python – Примеры выполнения HTTP запросов](https://python-scripts.com/requests)
*   [How to write a simple Postgres JSON wrapper with Python](https://levelup.gitconnected.com/how-to-write-a-simple-postgres-json-wrapper-with-python-ef09572daa66)
*   [How do I get JSON data from RESTful service using Python?](https://stackoverflow.com/questions/7750557/how-do-i-get-json-data-from-restful-service-using-python)
*   [Read JSON response in Python](https://stackoverflow.com/questions/33282067/read-json-response-in-python)

#### ORM:<a name="orm"></a>
*   SQLAlchemy
	-   [SQLAlchemy docs](https://docs.sqlalchemy.org/en/13/orm/tutorial.html)
	-	[Создание схемы базы данных в SQLAlchemy Core](https://pythonru.com/biblioteki/shemy-sqlalchemy-core)
	-   [SQLAlchemy для новичков](https://gadjimuradov.ru/post/sqlalchemy-dlya-novichkov/)
*   Peewee
	-   [peewee docs](http://docs.peewee-orm.com/en/latest/)
	-   [Peewee ORM манипуляция базами данных](https://python-scripts.com/peewee)


#### Перечень используемых модулей для Python:<a name="python"></a>
*	Python v. 3.5.3: 
	*   lxml - v. 4.6.3
	*   pytz - v. 2021.1
	*   numpy - v. 1.18.5
	*   python-dateutil - v. 2.8.1
	*   six - v. 1.15.0
	*   __pandas - v. 0.25.3__
	*   chardet - v. 4.0.0
	*   urllib3 - v. 1.26.4
	*   idna - v. 2.10
	*   certifi - v. 2020.12.5
	*   __requests - v. 2.25.1__ 
	*   psycopg2 - v. 2.8.6
	*   __SQLAlchemy - v. 1.3.24__
*   Python v. 3.9.0:
	*	python_dateutil - v. 2.8.1
	*	numpy - v. 1.20.3
	*	pytz - v. 2021.1
	*	six - v. 1.16.0
	*	__pandas - v. 1.2.4__
	*	idna - v. 2.10
	*	urllib3 - v. 1.26.5
	*	certifi - v. 2021.5.30
	*	chardet - v. 4.0.0
	*	__requests - v. 2.25.1__
	*	greenlet - v. 1.1.0
	*	__SQLAlchemy - v. 1.4.17__
	*	__psycopg2 - v. 2.8.6__


#### Ansible AWX:<a name="ansible"></a>
*	[Введение в автоматизацию с помощью Ansible](https://www.cisco.com/c/dam/m/ru_ru/training-events/2019/cisco-connect/pdf/introduction_automation_with_ansible_idrey.pdf)
*	[Полное руководство Ansible, 3 изд.](http://onreader.mdl.ru/MasteringAnsible.3ed/content/index.html#Preface)
*	[AWX Installation](https://github.com/ansible/awx/blob/devel/INSTALL.md)
*	[Ansible Tower (User Guide)](https://docs.ansible.com/ansible-tower/latest/html/userguide/index.html)
*	[Ansible Tower (Admin Guide)](https://docs.ansible.com/ansible-tower/latest/html/administration/index.html)
*	[AWX Google Groups](https://groups.google.com/forum/#!forum/awx-project)
*	[AWX Demo - 2 Hours - Matt Jones - Lead developer of tower. (Great for a technical understanding)](https://www.ansible.com/resources/webinars-training/awx-demo-2017)
*	[AWX Demo (Youtube) - No audio but quite useful for the initial setup (After installation)](https://www.youtube.com/watch?v=ZatqBgn_Wic&t=288s)
*	