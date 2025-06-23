#Import libraries
import sys
import os
import time
#from deltalake import write_deltalake, DeltaTable
#from deltalake.table import TableOptimizer
#from deltalake.exceptions import TableNotFoundError


# Add the path to the modules directory
sys.path.append('/Users/focus_profond/GIT_repo/flight_price_tracker')
#Importing personal modules
#from DF_functions import *
from OCR_google_flight import *
from Modules.metadata_functions import *


#extracting the last day of OCR
name_folder = '/Users/focus_profond/GIT_repo/flight_price_tracker/Data/raw/BigTable'
df = DeltaTable(name_folder).to_pandas()
last_day = max(df['date_of_search'])

# Executing OCR on each folder
raw_screenshots_path = '/Users/focus_profond/GIT_repo/flight_price_tracker/Data/raw/screenshots'
list_dates = os.listdir(raw_screenshots_path)
list_dates.remove('.DS_Store')
list_dates.sort()
days_to_extract =[]
for i in list_dates:
    if i > last_day:
        days_to_extract.append(i)
start_time = time.time()
time_per_days = {}
for date in days_to_extract:
        start_time_bis = time.time()
        day_folder = f'{raw_screenshots_path}/{date}/'
        trip_list = os.listdir(day_folder)
        try:
             trip_list.remove('.DS_Store')
        except:
             pass
        for trip in trip_list:
             big_df = ocr_on_folder(f'{raw_screenshots_path}/{date}/{trip}/')
             print(f'Dossier {raw_screenshots_path}/{date}/ traité par OCR.')
             storing_data(big_df,"/Data/raw/BigTable")
             print(f'Dossier {raw_screenshots_path}/{date}/ stocké dans deltatable.')
        end_time_bis = time.time()
        delta_time = round(end_time_bis - start_time_bis, 2)
        time_per_days[date]=delta_time
        
        

end_time = time.time()
print('totale duration : ',round(end_time - start_time, 2))
print('duration par jour : ', time_per_days)

