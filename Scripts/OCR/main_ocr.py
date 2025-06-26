#Import libraries
import sys
import os
import time

# Add the path to the modules directory
# ATTENTION ICI ON TRICHE CAR ON ECRIT LE CHEMIN EN DUR, NORMALEMENT ON DEVRAIT FAIRE CA DIFFERMEENT !!!
project_path = '/Users/focus_profond/GIT_repo/flight_price_tracker'
if project_path not in sys.path:
    sys.path.append(project_path)

#Importing personal modules and constants
from Modules.OCR_google_flight import *
from Modules.metadata_functions import *
from Config.constants import PATH
data_path = PATH['data_path']
log_path = PATH['logs_path']

#extracting the last day of OCR
name_folder = f'{data_path}/raw/BigTable'
df = DeltaTable(name_folder).to_pandas()
last_day = max(df['date_of_search'])
#print(last_day)

# Executing OCR on each folder
raw_screenshots_path = f'{data_path}/raw/screenshots'
list_dates = os.listdir(raw_screenshots_path)
list_dates.remove('.DS_Store')
list_dates.sort()
days_to_extract =[]
for i in list_dates:
    if i > last_day:
        days_to_extract.append(i)
#print(days_to_extract)
#print(s)
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
             big_df = ocr_on_folder(f'{raw_screenshots_path}/{date}/{trip}/', trip, date)
             print(f'Folder {raw_screenshots_path}/{date}/ processed by OCR.')
             storing_data(big_df,"/Data/raw/BigTable")
             print(f'Folder {raw_screenshots_path}/{date}/ stored in deltatable.')
        end_time_bis = time.time()
        delta_time = round(end_time_bis - start_time_bis, 2)
        time_per_days[date]=delta_time
        
        

end_time = time.time()
total_duration = round(end_time - start_time, 2)
duration_per_day = time_per_days
print('totale duration : ',round(end_time - start_time, 2))
print('duration par jour : ', time_per_days)


#storing the logs
status_log =  log_path+'/main_ocr.log'   
timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
with open(status_log, "a") as f:
    f.write(f"{timestamp} - {len(days_to_extract)} days - {total_duration} en secondes.\n")
    f.write(f"{time_per_days}")

