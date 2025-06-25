import psycopg2
from io import StringIO

# API AUTHORIZATION CONFIG
from configparser import ConfigParser
# Load DB credentials from configuration file
parser = ConfigParser()
CONFIGFILE = '/Users/focus_profond/GIT_repo/flight_price_tracker/Config/pipeline.conf'
parser.read(CONFIGFILE)
db_credentials = parser["postgresql_db"]

#DB credentials
DB_HOST = db_credentials['DB_HOST']
DB_PORT = db_credentials['DB_PORT']
DB_NAME = db_credentials['DB_NAME']
DB_USER = db_credentials['DB_USER_python']
DB_PASS = db_credentials['DB_PASS_python']

def connection_to_postgresql(db_host=DB_HOST, db_port=DB_PORT,db_name = DB_NAME, db_user = DB_USER, db_pass =DB_PASS):
    """
    Fonction qui se connecte à une base de données PostgreSQL et renvoie un curseur.
    """
    DB_HOST = db_credentials['DB_HOST']
    DB_PORT = db_credentials['DB_PORT']
    DB_NAME = db_credentials['DB_NAME']
    DB_USER = db_credentials['DB_USER_python']
    DB_PASS = db_credentials['DB_PASS_python']
    conn = psycopg2.connect(
    database = DB_NAME
	,user= DB_USER
	,password= DB_PASS
	,host= DB_HOST
	,port= DB_PORT
    )   
    cur = conn.cursor()
    print("📌 Base utilisée :", conn.get_dsn_parameters().get('dbname'))
    
    return (cur,conn)#{'cur':cur,'conn':conn}

def request_query(query, cur, type='fetchall'):
    cur.execute(query)
    if type=='fetchone':
        records = cur.fetchone()
    elif type == 'fetchmany':
        records = cur.fetchmany()
    else:
        records = cur.fetchall()
    return records

def execute_query(query,cur,conn):
    cur.execute(query)
    conn.commit()
    print('Query executed correctly')
    return True

def copying_data(df,db_table, cur,conn,db_schema=None):
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    try: 
        if db_schema != None:
            copy_query = f"""COPY "{db_schema}".{db_table} ({', '.join(df.columns)}) FROM STDIN WITH CSV"""
        else:
            copy_query = f"""COPY {db_table} ({', '.join(df.columns)}) FROM STDIN WITH CSV"""
        cur.copy_expert(copy_query, buffer)
        conn.commit()
        print("new data inserted correctly.")
        return True
    except Exception as e:
        print(f'error in inserting the data of the DF inside the {db_table}')
        raise


def closing_connection(conn, cur):
    cur.close()
    conn.close()
    print("DB closed")
    return True
