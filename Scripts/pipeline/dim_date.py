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
from Modules.dimensions_functions import *
data_path = PATH['data_path']
log_path = PATH['logs_path']

def main():
    # CONNECTION TO THE DB
    cur, my_conn = connection_to_postgresql()

    #creating the dataframe
    first_date = '2010-01-01'
    last_date = '2050-01-01'
    DB_SCHEMA ="fpt"

    df_dim_date = create_dim_date(first_date,last_date)

    csv_path = '/Users/focus_profond/GIT_repo/flight_price_tracker/Data/dim_files/all_dim.xlsx'
    df_dim_season = pd.read_excel(csv_path, sheet_name='dim_season')
    df_dim_date = df_dim_date.merge(df_dim_season, left_on='month_name',right_on='month', how="left")
    df_dim_date= df_dim_date.rename(columns={'month_x':'month'})
    df_dim_date= df_dim_date.drop(columns=['month_y'])

    #INSERTING ROWS IN THE DIM DATE TABLE
    try:
        copying_data(df_dim_date,'dim_date',cur,my_conn,db_schema=DB_SCHEMA)
        pass
    except Exception as e:
            print("An error occured while cpoying the data into the postgresql table.",e)
    df_dim_date.head()

    #commiting and closing
    my_conn.commit()
    cur.close()
    my_conn.close()


if __name__ == "__main__":
    main()

