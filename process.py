import pandas as pd
import hashlib

def get_hashcode(columns):
    col_str = ""
    for col in columns:
        col_str = col_str+col
    hashcode = hashlib.md5(col_str.encode())
    return hashcode.hexdigest()

def get_new_rows(source_df,target_df):
    
    source_target_join = pd.merge(source_df,target_df,left_on='customer_id_src',right_on='customer_id_trg',how='left')
    source_target_join['new_row'] = source_target_join.apply(lambda x:'Y' if pd.isnull(x['customer_id_trg']) else 'N',axis=1)
    new_rows = source_target_join[source_target_join['new_row']=='Y']
    new_ids = new_rows['customer_id_src']
    return new_ids

def get_updated_rows(source_df,target_df):
    source_df['hashcode_src'] = source_df.apply(lambda x:get_hashcode([x['first_name_src'],x['last_name_src'],x['country_src']]),axis=1)
    source_target_join = pd.merge(source_df,target_df,left_on='customer_id_src',right_on='customer_id_trg',how='left')
    source_target_join['updated'] = source_target_join.apply(lambda x:'Y' if(x['hashcode_src']!=x['hashcode_trg'] and pd.notnull(x['hashcode_trg'])) else 'N',axis=1)
    updated_rows = source_target_join[source_target_join['updated']=='Y']
    updated_ids = updated_rows['customer_id_src']
    return updated_ids

def process_data(source_df,target_df):

    if(target_df.shape[0]>0):
        source_columns = source_df.columns
        target_columns = target_df.columns

        source_columns_new = [column + "_src" for column in source_df.columns]
        source_df.columns = source_columns_new
    
        target_columns_new = [column + "_trg" for column in target_df.columns]
        target_df.columns = target_columns_new
        
        new_ids = get_new_rows(source_df,target_df)
        updated_ids = get_updated_rows(source_df,target_df)
        
        del(source_df['hashcode_src'])
        source_df.columns=source_columns
        target_df.columns=target_columns

        new_rows = pd.merge(source_df,new_ids,left_on='customer_id',right_on='customer_id_src',how='inner')
        updated_rows = pd.merge(source_df,updated_ids,left_on='customer_id',right_on='customer_id_src',how='inner')
        del(new_rows['customer_id_src'])
        del(updated_rows['customer_id_src'])

        return new_rows,updated_rows

    
    
