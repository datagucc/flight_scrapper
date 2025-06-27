import sys
#import os
#import time
#from io import StringIO

# Add the path to the modules directory
# ATTENTION ICI ON TRICHE CAR ON ECRIT LE CHEMIN EN DUR, NORMALEMENT ON DEVRAIT FAIRE CA DIFFERMEENT !!!
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
    #We get the big table from the raw layer
    name_folder_raw = f"{data_path}/raw/BigTable" 
    df_raw = DeltaTable(name_folder_raw).to_pandas()
    #to test if the merging is correct
    #df_raw = df_raw.loc[df_raw['date_of_search']< '2025-06-17']

    #We add the id column to the table
    df_raw = df_raw.sort_index(ascending=False)
    df_raw = df_raw.reset_index(drop=True) # to assure that the index are consecuitives. 
    df_raw['id']= df_raw.index+1  # Starting from 1, not 0.
    maxid_raw = max(df_raw['id'])
    maxdate_raw = max(df_raw['date_of_search'])
    size_raw = df_raw.shape[0]
    

    #We insert the new lines into the big table of the bronze layer
    # MERGING THE SILVER BIG TABLE DEPENDING ON THE ID
    name_folder = f"{data_path}/bronze/BigTable" 
    df_bigtable = DeltaTable(name_folder).to_pandas()
    try:
        maxid_bt = max(df_bigtable['id'])
        maxdate_bt = max(df_bigtable['date_of_search'])
    except:
        maxid_bt = 0
        maxdate_bt = '1900-01-01'
    size_bt = df_bigtable.shape[0]
    partition_cols = None
    predicate = "target.id = source.id"
    source = 'ocr_layer'
    author = 'Augustin'
    save_new_data_as_delta(df_raw,name_folder,predicate= predicate, partition_cols=partition_cols, layer = 'Bronze', source= source, author =author)
    
    #print('----------------------------------------------')
    #print('--------- METADATA OF BRONZE LAYER --------------')
    #print(f"Size of the raw big table : {size_raw} rows.")
    #print(f"Max date of the raw big table : {maxdate_raw}.")
    #print(f"Max id of the raw big table : {maxdate_raw}.")
    #print(f"Size of the big table : {size_bt} rows.")
    #print(f"Max date of the big table : {maxdate_bt}.")
    #print(f"Max id of the big table : {maxdate_bt}.")
    
    return (f"""
            ----------------------------------------------\n
            --------- METADATA OF BRONZE LAYER --------------\n
            Size of the raw big table : {size_raw} rows.\n
            Max date of the raw big table : {maxdate_raw}.\n
            Max id of the raw big table : {maxid_raw}.\n
            Size of the big table : {size_bt} rows.\n
            Max date of the big table : {maxdate_bt}.\n
            Max id of the big table : {maxid_bt}.\n
""")

if __name__ == "__main__":
    main()