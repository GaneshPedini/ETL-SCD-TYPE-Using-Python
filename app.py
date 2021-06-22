from config import DB_DETAIL
from read import read_source, read_target
from process import process_data
import pandas as pd
from sqlalchemy import create_engine

source_connection_string = "postgresql://"+DB_DETAIL['dev']['source_db']['DB_USER']+":"+DB_DETAIL['dev']['source_db']['DB_PASS']+"@"+DB_DETAIL['dev']['source_db']['DB_HOST']+"/"+DB_DETAIL['dev']['source_db']['DB_NAME']
target_connection_string = "postgresql://"+DB_DETAIL['dev']['target_db']['DB_USER']+":"+DB_DETAIL['dev']['target_db']['DB_PASS']+"@"+DB_DETAIL['dev']['target_db']['DB_HOST']+"/"+DB_DETAIL['dev']['target_db']['DB_NAME']

source_engine = create_engine(source_connection_string)
target_engine = create_engine(target_connection_string)


source_df = read_source(source_engine)
target_df = read_target(target_engine)


new_rows,updated_rows = process_data(source_df,target_df)
#print("New inserted rows in source:",end='\n')
#print("-------------------------------------")
#print(new_rows.head())
#print("-------------------------------------")
#print("updated rows in source:", end='\n')
#print("-------------------------------------")
#print(updated_rows.head())






