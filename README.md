# json_parser - проект создания ETL-инструмента для загрузки, преобразования и выгрузки данных информационных витрин.<br>
**Источник данных:** web-сервисы метаданных и данных витрин<br>
**Преобразование:** парсинг JSON, проверки и преобразования типов данных, импорт в Pandas DataFrame<br>
**Получатель данных:** БД PostgreSQL<br>
**Формат источника данных:** JSON

## Перечень файлов проекта:
*   config.txt - файл конфигурации
*   debug.log - журнал (создается и наполняется в процессе работы программы)
*   main.py - главный файл программы
*   log_settings.py - модуль настроек логгеров
*   json_proc.py - модуль получения параметров конфигурации (ссылок на ресурсы витрин) и базового класса витрины
*   db_mirror.py - модуль для работы с БД PostgreSQL
*   utils.py - модуль со вспомогательными инструментами (функциями/методами):

## Полезные ссылки (по разделам):
*   [Python (install/update)](#python)
*   [REST API, Requests, JSON response](#requests)
*   [Object Relational Mapping (ORM)](#orm)
*   [Версия интерпретатора Python и перечень используемых модулей](#python)
*	[Ansible/AWX](#ansible)

#### Python (install/update):<a name="python"></a>
*	[How to install Python on Linux](https://opensource.com/article/20/4/install-python-linux)
*	[Установка Python 3.9 на Ubuntu 20.04 LTS](https://setiwik.ru/ustanovka-python-3-9-na-ubuntu-20-04-lts/)
*	[Unable to set default python version to python3 in ubuntu](https://stackoverflow.com/questions/41986507/unable-to-set-default-python-version-to-python3-in-ubuntu)
*	[How to install Python packages from the tar.gz file without using pip install](https://stackoverflow.com/questions/36014334/how-to-install-python-packages-from-the-tar-gz-file-without-using-pip-install)
*	[Python3: ImportError: No module named '_ctypes' when using Value from module multiprocessing](https://stackoverflow.com/questions/27022373/python3-importerror-no-module-named-ctypes-when-using-value-from-module-mul)

#### REST API, Requests, JSON response:<a name="request"></a>
*   [REST API: что это простыми словами: принципы, стандарты, описание](https://boodet.online/reastapi)
*   [Requests в Python – Примеры выполнения HTTP запросов](https://python-scripts.com/requests)
*   [How to write a simple Postgres JSON wrapper with Python](https://levelup.gitconnected.com/how-to-write-a-simple-postgres-json-wrapper-with-python-ef09572daa66)
*   [How do I get JSON data from RESTful service using Python?](https://stackoverflow.com/questions/7750557/how-do-i-get-json-data-from-restful-service-using-python)
*   [Read JSON response in Python](https://stackoverflow.com/questions/33282067/read-json-response-in-python)
*	Для загрузки страницы через webdriver браузера Chrome, необходимо скачать [chromedriver](https://chromedriver.storage.googleapis.com/index.html?path=87.0.4280.88/)

#### ORM:<a name="orm"></a>
*   SQLAlchemy
	-   [SQLAlchemy docs](https://docs.sqlalchemy.org/en/13/orm/tutorial.html)
	-	[Создание схемы базы данных в SQLAlchemy Core](https://pythonru.com/biblioteki/shemy-sqlalchemy-core)
	-   [SQLAlchemy для новичков](https://gadjimuradov.ru/post/sqlalchemy-dlya-novichkov/)
	-   [Gentle 'drop tables' using sqlalchemy](https://gist.github.com/riffm/1678194)
*   Peewee
	-   [peewee docs](http://docs.peewee-orm.com/en/latest/)
	-   [Peewee ORM манипуляция базами данных](https://python-scripts.com/peewee)

#### Ansible AWX:<a name="ansible"></a>
*	[Введение в автоматизацию с помощью Ansible](https://www.cisco.com/c/dam/m/ru_ru/training-events/2019/cisco-connect/pdf/introduction_automation_with_ansible_idrey.pdf)
*	[Полное руководство Ansible, 3 изд.](http://onreader.mdl.ru/MasteringAnsible.3ed/content/index.html#Preface)
*	[AWX Installation](https://github.com/ansible/awx/blob/devel/INSTALL.md)
*	[Ansible Tower (User Guide)](https://docs.ansible.com/ansible-tower/latest/html/userguide/index.html)
*	[Ansible Tower (Admin Guide)](https://docs.ansible.com/ansible-tower/latest/html/administration/index.html)
*	[AWX Google Groups](https://groups.google.com/forum/#!forum/awx-project)
*	[AWX Demo - 2 Hours - Matt Jones - Lead developer of tower. (Great for a technical understanding)](https://www.ansible.com/resources/webinars-training/awx-demo-2017)
*	[AWX Demo (Youtube) - No audio but quite useful for the initial setup (After installation)](https://www.youtube.com/watch?v=ZatqBgn_Wic&t=288s)

#### Перечень используемых модулей для Python:<a name="python"></a>
*	***Python v. 3.5.3***: 
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
*   ***Python v. 3.9.0***:
	*	six - v. 1.16.0
	*	python_dateutil - v. 2.8.1
	*	numpy - v. 1.20.3
	*	pytz - v. 2021.1
	*	__pandas - v. 1.2.4__

	*	idna - v. 2.10
	*	urllib3 - v. 1.26.5
	*	certifi - v. 2021.5.30
	*	chardet - v. 4.0.0
	*	__requests - v. 2.25.1__

	*	greenlet - v. 1.1.0
	*	__SQLAlchemy - v. 1.4.17__

	*	__psycopg2_binary__ - v. 2.8.6

	*	cssselect - v. 1.1.0
	*	soupsieve - v. 2.2.1
	* 	__beautifulsoup4__ - v. 4.9.3

	*	__selenium__ - v. 3.141.0

	*	appdirs - v. 1.4.4
	*	pyee - v. 8.1.0 
	*	websockets - v. 8.1
	*	tqdm - v. 4.61.0
	*	lxml - v. 4.6.3
	*	pyquery - v. 1.4.3
	*	parse - v. 1.19.0
	*	bs4 - v. 0.0.1
	*	pyppeteer - v. 0.2.5
	*	w3lib - v. 1.22.0
	*	fake-useragent - v. 0.1.11
	*	__requests_html__ - v. 0.10.0

	



#### Перечень возможных доработок:<a name="modification"></a>
*	Загрузка данных из json в pandas DataFrame без принудительной типизации, все столбцы сделать текстовыми и разбирать в модуле db_mirror.py
*	

