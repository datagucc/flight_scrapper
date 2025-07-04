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
import geopy.distance
import pycountry
import pycountry_convert

data_path = PATH['data_path']
log_path = PATH['logs_path']

def main():
    # CONNECTION TO THE DB
    DB_SCHEMA = 'fpt'
    cur, my_conn = connection_to_postgresql()

    # DIM CITY  : FOR NOW, IT IS USELESS TO CREATE A TABLE OF IT AS THE INFOS WILL BE STORED IN DIM TRIP
    # WE CAN CONSIDER IT AS A TEMPORARY TABLE
    csv_path = '/Users/focus_profond/GIT_repo/flight_price_tracker/Data/dim_files/dim_city.csv'
    dim_city = pd.read_csv(csv_path, sep=';')
    #finding hemisphere
    dim_city['hemisphere']= dim_city.apply(lambda row: 'Nord' if row['Latitude']>10 else ('Sud' if row['Latitude']<-10 else 'Central' ),axis=1)
    #finding continent
    dim_city['continent'] = dim_city.apply(lambda row: country_to_continent(row['Country Name']), axis=1)
    #removing useless columns
    dim_city = dim_city.drop(columns=['Airport Name', 'Latitude','Country Code' ,'Longitude', 'World Area Code', 'City Name geo_name_id','Country Name geo_name_id'])

    #renamming
    rename_mapping = {
        'Airport Code':'IATA_code'
        ,'Country Name':'country'
        ,'City Name':'city'

    }
    dim_city = dim_city.rename(columns= rename_mapping)


    #DIM TRIP
    data_path ='/Users/focus_profond/GIT_repo/flight_price_tracker/Data/dim_files/all_dim.xlsx'
    excel_path = data_path
    dim_trip = pd.read_excel(excel_path, sheet_name='dim_trip_url')
    #getting the ori and desti IATA Code
    dim_trip[['codeIATA_ori','codeIATA_desti']] = dim_trip['trip'].str.split('_', expand=True)

    #merging for ori
    dim_trip = dim_trip.merge(dim_city, left_on='codeIATA_ori',right_on='IATA_code', how="left")
    rename_mapping = {
        'city':'city_ori'
        ,'country':'country_ori'
        ,'coordinates':'coordinates_ori'
        ,'continent':'continent_ori'
        ,'hemisphere':'hempisphere_ori'
    }
    dim_trip = dim_trip.rename(columns= rename_mapping)
    #merging for desti
    dim_trip = dim_trip.merge(dim_city, left_on='codeIATA_desti',right_on='IATA_code', how="left")
    rename_mapping = {
        'city':'city_desti'
        ,'country':'country_desti'
        ,'coordinates':'coordinates_desti'
        ,'continent':'continent_desti'
        ,'hemisphere':'hempisphere_desti'
    }
    dim_trip = dim_trip.rename(columns= rename_mapping)
    dim_trip = dim_trip.drop(columns =['IATA_code_x','IATA_code_y'])

    #computing the km
    dim_trip['distance_km']= dim_trip.apply(lambda row:geopy.distance.distance(row['coordinates_ori'],row['coordinates_desti']).kilometers, axis = 1 )

    #defining type of flight duration
    dim_trip['type_flight_duration']= dim_trip.apply(lambda row: get_type_hault(row['distance_km']),axis=1)

    #defining type of flight
    dim_trip['type_flight']= dim_trip.apply(lambda row: 'domestic' if row['country_ori']== row['country_desti'] else ('continental' if row['continent_ori']==row['continent_desti'] else 'intercontinental') ,axis=1)

    #removing uneccesary columns
    dim_trip = dim_trip.drop(columns=['url','coordinates_ori','coordinates_desti'])


    #inserting the DF
    try:
        copying_data(dim_trip,'dim_trip',cur,my_conn,db_schema=DB_SCHEMA)
        #pass
    except Exception as e:
        print("An error occured while cpoying the data into the postgresql table.",e)
    

    #commiting and closing
    my_conn.commit()
    cur.close()
    my_conn.close()


if __name__ == "__main__":
    main()




