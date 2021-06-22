import os

DB_DETAIL = {

'dev':{

'source_db':{
            'DB_TYPE': 'postgres',
            'DB_HOST': 'localhost',
            'DB_NAME': 'python',
            'DB_USER': 'postgres',#os.environ.get('RETAIL_DB_USER'),
            'DB_PASS': 'postgres'#os.environ.get('RETAIL_DB_PASS')
},
'target_db':{
            'DB_TYPE': 'postgres',
            'DB_HOST': 'localhost',
            'DB_NAME': 'python',
            'DB_USER': 'postgres',#os.environ.get('RETAIL_DB_USER'),
            'DB_PASS': 'postgres'#os.environ.get('RETAIL_DB_PASS')
}

},
'cert':{},
'prod':{}
}