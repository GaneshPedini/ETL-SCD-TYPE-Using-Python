import pandas as pd
from sqlalchemy import select, Table, MetaData



def read_source(source_engine):
    data = pd.read_sql_query("SELECT customer_id, first_name, last_name, company_name, address, country, city, county, state, zip, phone1, phone2, email, web, postal, post, province	FROM public.customer_source;",source_engine)
    return data


def read_target(target_engine):
    data = pd.read_sql_query("SELECT customer_key, customer_id, first_name, last_name, company_name, address, country, city, state, zip, phone1, phone2, email, web, hashcode, active_flag	FROM public.customer_target_scd where active_flag=1;",target_engine)
    return data