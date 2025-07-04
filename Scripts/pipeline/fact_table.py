import sys
#import os
#import time
project_path = '/Users/focus_profond/GIT_repo/flight_price_tracker'
if project_path not in sys.path:
    sys.path.append(project_path)
#Importing personal modules
from Config.constants import PATH
from Modules.metadata_functions import *
from Modules.DF_functions import *
from Modules.postgresql_utils import *
data_path = PATH['data_path']
log_path = PATH['logs_path']


def main():
    # CONNECTION TO THE DB
    cur, my_conn = connection_to_postgresql()

    #Getting the max(id) of the big table of the postgresql database
    DB_SCHEMA = "FPT"  #flight price tracker
    DB_TABLE = "fact_table"
    maxid_query = f"""SELECT max(date_of_search) max_date
                            ,max(id) max_id 
            FROM {DB_SCHEMA}.{DB_TABLE};"""
        
    try:
        records = request_query(maxid_query,cur,type='fetchone')
        maxdate = records[0]
        maxid_db = records[1]
    except:
            maxdate='1900-01-01'
            maxid_db = 0
            
    #We get the big table from the silver layer
    name_folder = f"{data_path}/silver/BigTable" 
    df_silver = DeltaTable(name_folder).to_pandas()
    nb_of_rows_silver = df_silver.shape[0]

    #We filter from the id, so the transformation will cost less in computation.
    #maxid = 0
    if maxid_db == None:
        maxid_db = 0
    df_silver = df_silver.loc[df_silver['id']> maxid_db]

    df_silver["flight_date"] = pd.to_datetime(df_silver["flight_date"])
    df_silver["date_of_search"] = pd.to_datetime(df_silver["date_of_search"])
    df_silver["days_before_flight"] = (df_silver["flight_date"] - df_silver["date_of_search"]).dt.days

    #filtering the data to improve the data quality :   
    df_silver = df_silver.loc[df_silver['flight_price'] != '-1']
    df_silver = df_silver.loc[df_silver['flight_date'] != '2099-12-31'] 
    #removing useless columns 
    df_silver = df_silver.drop(columns=['country_ori','country_desti','city_ori','city_desti','codeIATA_ori','codeIATA_desti'])
    nb_of_new_rows = len(df_silver) 

    # INSERTING ROW INTO THE FACT TABLE
    try:
        copying_data(df_silver,DB_TABLE,cur,my_conn,db_schema=DB_SCHEMA)
    except Exception as e:
        print("An error occured while cpoying the data into the postgresql table.",e)

    #commiting and closing
    my_conn.commit()
    cur.close()
    my_conn.close()

    return maxid_db, nb_of_rows_silver, nb_of_new_rows

if __name__ == "__main__":
    main()