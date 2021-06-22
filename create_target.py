from config import DB_DETAIL
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, String, Integer, Float
from sqlalchemy import inspect

connection_string = "postgresql://"+DB_DETAIL['dev']['target_db']['DB_USER']+":"+DB_DETAIL['dev']['target_db']['DB_PASS']+"@"+DB_DETAIL['dev']['target_db']['DB_HOST']+"/"+DB_DETAIL['dev']['target_db']['DB_NAME']
engine = create_engine(connection_string)
metadata = MetaData(engine)

if not inspect(engine).has_table('customer_target_scd'):
    Table('customer_target_scd',
    metadata,
    Column('customer_key', Integer, primary_key=True),
    Column('customer_id', Integer),
    Column('first_name', String),
    Column('last_name', String),
    Column('company_name', String),
    Column('address', String),
    Column('country', String),
    Column('state', String),
    Column('zip', String),
    Column('phone1', String),
    Column('email', String),
    Column('active_flag', Integer),
    Column('hashcode', String),
    )
    metadata.create_all()

