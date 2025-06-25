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
    #We get the big table from the raw layer
    name_folder = '/Users/focus_profond/GIT_repo/flight_price_tracker/Data/raw/BigTable'
    df_raw = DeltaTable(name_folder).to_pandas()
    #to test if the merging is correct
    #df_raw = df_raw.loc[df_raw['date_of_search']< '2025-06-17']

    #We add the id column to the table
    df_raw = df_raw.sort_index(ascending=False)
    df_raw = df_raw.reset_index(drop=True) # pour s'assurer que les index sont consécutifs
    df_raw['id']= df_raw.index+1  # Commence à 1 (ou à 0 si tu préfères)

    #We insert the new lines into the big table of the bronze layer
    # MERGING THE SILVER BIG TABLE DEPENDING ON THE ID
    name_folder = '/Users/focus_profond/GIT_repo/flight_price_tracker/Data/bronze/BigTable'
    partition_cols = None
    predicate = "target.id = source.id"
    source = 'ocr_layer'
    author = 'Augustin'
    save_new_data_as_delta(df_raw,name_folder,predicate= predicate, partition_cols=partition_cols, layer = 'Bronze', source= source, author =author)


if __name__ == "__main__":
    main()