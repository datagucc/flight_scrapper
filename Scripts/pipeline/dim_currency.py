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
    DB_SCHEMA='fpt'
    # CONNECTION TO THE DB
    cur, my_conn = connection_to_postgresql()
    
    # Freecurrency API configuration
    api_credentials = parser["freecurrency_api"]
    access_token = api_credentials["access_token"]
    base_url = api_credentials["base_url"]
    #calling the endpoint
    base_currency = 'EUR'
    currencies = 'EUR,USD,CAD'
    params ={
            'apikey':access_token
        # ,'currencies':currencies
            ,'base_currency':base_currency
        }
    headers={}
    endpoint = "latest"
    json_data = get_data(base_url, endpoint, data_field='data', params = params,headers=headers)

    #creating the DF
    lastest_rates = pd.DataFrame.from_dict(json_data[0],orient='index')
    lastest_rates['base_currency'] = base_currency
    lastest_rates = lastest_rates.reset_index()
    lastest_rates = lastest_rates.rename(columns={0:'exchange_rate','index':'currencies'})
    lastest_rates['day_of_rates'] = datetime.now().strftime('%Y-%m-%d')

    #inserting the DF
    try:
        copying_data(lastest_rates,'dim_currency',cur,my_conn,db_schema=DB_SCHEMA)
        #pass
    except Exception as e:
        print("An error occured while cpoying the data into the postgresql table.",e)
    lastest_rates.head()



    #commiting and closing
    my_conn.commit()
    cur.close()
    my_conn.close()


if __name__ == "__main__":
    main()