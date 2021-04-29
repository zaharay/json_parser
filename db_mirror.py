# (venv)>pip install SQLAlchemy
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime

engine = create_engine('postgresql://admin:zahar@localhost:5432/mirror', echo=True)
meta = MetaData()


