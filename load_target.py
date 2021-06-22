from app import new_rows,updated_rows,target_engine,target_df
import pandas as pd
from process import get_hashcode
from sqlalchemy import MetaData, Table, update

metadata = MetaData(target_engine)


def update_flag():
    with target_engine.connect() as con:
        target_table =  Table("customer_target_scd", metadata, autoload_with=target_engine)
        for ind,row in updated_rows.iterrows():
            update_statement = update(target_table).where(target_table.c.customer_id==row.customer_id).values(active_flag=0)
            con.execute(update_statement)
        
def full_load():
    source_data = pd.read_sql_query("SELECT customer_id, first_name, last_name, company_name, address, country, city, state, zip, phone1, phone2, email, web FROM public.customer_source",target_engine)
    source_data['hashcode'] = source_data.apply(lambda x:get_hashcode([x['first_name'],x['last_name'],x['country']]),axis=1)
    source_data['active_flag'] = 1
    source_data.to_sql('customer_target_scd',con=target_engine, if_exists='append', index=False)

def insert_new_record():
    new_records = new_rows[['customer_id', 'first_name', 'last_name', 'company_name', 'address', 'country', 'city', 'state', 'zip', 'phone1', 'phone2', 'email', 'web']]
    #print(new_records.head())
    new_records['hashcode'] = new_records.apply(lambda x:get_hashcode([x['first_name'],x['last_name'],x['country']]),axis=1)
    new_records['active_flag'] = 1
    new_records.to_sql('customer_target_scd',con=target_engine, if_exists='append', index=False)


def insert_updated_record():
    update_flag()
    update_records = updated_rows[['customer_id', 'first_name', 'last_name', 'company_name', 'address', 'country', 'city', 'state', 'zip', 'phone1', 'phone2', 'email', 'web']]
    update_records['hashcode'] = update_records.apply(lambda x:get_hashcode([x['first_name'],x['last_name'],x['country']]),axis=1)
    update_records['active_flag'] = 1
    update_records.to_sql('customer_target_scd',con=target_engine, if_exists='append', index=False)


if(target_df.shape[0]>0):
    if(new_rows.shape[0]>0):
        insert_new_record()
    
    if(updated_rows.shape[0]>0):
        insert_updated_record()
      
else:
    full_load()

    



