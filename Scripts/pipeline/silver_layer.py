import sys
#import os
#import time
#from io import StringIO
project_path = '/Users/focus_profond/GIT_repo/flight_price_tracker'
if project_path not in sys.path:
    sys.path.append(project_path)
#Importing personal modules
from Config.constants import PATH
from Modules.metadata_functions import *
from Modules.DF_functions import *
import psycopg2
data_path = PATH['data_path']
log_path = PATH['logs_path']



def main():
    #We get the max(id) of the big table from the silver layer to filter our bronze layer.
    name_folder = f"{data_path}/silver/BigTable" 
    df_silver = DeltaTable(name_folder).to_pandas()
    try:
        maxid_silver = max(df_silver['id'])
    except:
        maxid_silver = 0

    nb_of_rows_silver = df_silver.shape[0]

    #We get the big table from the bronze layer
    name_folder = f"{data_path}/bronze/BigTable" 
    df_bronze = DeltaTable(name_folder).to_pandas()
    #We filter from the id, so the transformation will cost less in computation.
    if maxid_silver == None:
        maxid_silver = 0
    #print(maxid)
    nb_of_rows_bronze = df_bronze.shape[0]
    df_bronze = df_bronze.loc[df_bronze['id']> maxid_silver]
    #print(len(df_bronze))
    nb_of_new_rows = len(df_bronze)
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
    xlsx_path  = f"{project_path}/Config/trip_config.xlsx" 
    df_loca = pd.read_excel(xlsx_path, sheet_name='good_one')
    df_bronze = pd.merge(df_bronze, df_loca, how='left', on='trip')
    df_bronze = df_bronze.drop(columns = ['url'])



    # MERGING THE SILVER BIG TABLE 
    name_folder = f"{data_path}/silver/BigTable" 
    partition_cols = None
    predicate = "target.id = source.id"
    source = 'bronze_layer'
    author = 'Augustin'
    save_new_data_as_delta(df_bronze,name_folder,predicate= predicate, partition_cols=partition_cols, layer = 'Silver', source= source, author =author)

   # print('----------------------------------------------')
    #print('--------- METADATA OF SILVER LAYER --------------')
    #print(f"Size of the bronze table : {nb_of_rows_bronze} rows.")
    #print(f"Size of the silver table : {nb_of_rows_silver} rows.")
    #print(f"Nb of new rows  : {nb_of_new_rows}.")
    #print(f"Nb of price errors : {nb_of_price_errors}.")
    #print(f"Nb of date errors : {nb_of_date_errors}.")
    return (f"""
            ----------------------------------------------\n
            --------- METADATA OF SILVER LAYER --------------\n
            Size of the bronze table : {nb_of_rows_bronze} rows.\n
            Size of the silver table : {nb_of_rows_silver} rows.\n
            Nb of new rows  : {nb_of_new_rows}.\n
            Nb of price errors : {nb_of_price_errors}.\n
            Nb of date errors : {nb_of_date_errors}.\n
""")


if __name__ == "__main__":
    main()