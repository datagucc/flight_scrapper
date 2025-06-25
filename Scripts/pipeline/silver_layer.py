import sys
import os
import time
#pour insérer des lignes dans une DB 
from io import StringIO
sys.path.append('/Users/focus_profond/GIT_repo/flight_price_tracker')
#Importing personal modules
from Modules.metadata_functions import *
from Modules.DF_functions import *
import psycopg2


def main():
    #We get the max(id) of the big table from the silver layer to filter our bronze layer.
    name_folder = '/Users/focus_profond/GIT_repo/flight_price_tracker/Data/silver/BigTable'
    df_silver = DeltaTable(name_folder).to_pandas()
    maxid = max(df_silver['id'])

    #We get the big table from the bronze layer
    name_folder = '/Users/focus_profond/GIT_repo/flight_price_tracker/Data/bronze/BigTable'
    df_bronze = DeltaTable(name_folder).to_pandas()
    #We filter from the id, so the transformation will cost less in computation.
    if maxid == None:
        maxid = 0
    #print(maxid)
    df_bronze = df_bronze.loc[df_bronze['id']> maxid]
    print(len(df_bronze))
    # we clean the data : 2099-12-31 for empty date and -1 for empty price
    #we clean the price column to get only the price
    df_bronze['flight_price'] = df_bronze['flight_price'].str.replace(r"\D","", regex=True)
    df_bronze.loc[df_bronze['flight_price'].str.strip() == '', 'flight_price'] = '-1'
    # we clean the flight date column to fill the empty rows
    df_bronze['flight_date'] = df_bronze["flight_date"].fillna("2099-12-31")
    nb_of_price_errors = len(df_bronze[df_bronze["flight_price"] == "-1"])
    nb_of_date_errors = len(df_bronze[df_bronze["flight_date"] == "2099-12-31"])

    # we enrich the data :
    # adding the currency column 
    df_bronze['currency'] = '€'
    # adding complementary columns : origin_city ; destination_city; origin_country ; destination_country
    # MERGING !!
    # FULL PATH NOT RELATIVE PATH
    df_loca = pd.read_excel("/Users/focus_profond/GIT_repo/flight_price_tracker/Config/trip_config.xlsx", sheet_name='good_one')
    df_bronze = pd.merge(df_bronze, df_loca, how='left', on='trip')
    df_bronze = df_bronze.drop(columns = ['url'])



    # MERGING THE SILVER BIG TABLE 
    name_folder = '/Users/focus_profond/GIT_repo/flight_price_tracker/Data/silver/BigTable'
    partition_cols = None
    predicate = "target.id = source.id"
    source = 'bronze_layer'
    author = 'Augustin'
    save_new_data_as_delta(df_bronze,name_folder,predicate= predicate, partition_cols=partition_cols, layer = 'Silver', source= source, author =author)


if __name__ == "__main__":
    main()