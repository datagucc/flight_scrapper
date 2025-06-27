#Import libraries
import sys
import os
import time
import datetime

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

# To be sure that the last day executed by the OCR has been executed and completed during the last run
#  we compare the nb of folders of the last day with the number of distinct trips in the bigtable (as each trip of a particular day represents a folder).
# this way, we are sure that the last day has been fully and successfully processed by the OCR.
# In case of a failure, the only thing that could have happened is that only the last folder of the last day
# has been processed halfway (which is not a big issue in comparaison of the data loss).


# Another way around would be to also create a log file for each day processed by the OCR. (and therefore change the way
# we are storing the log of the OCR. Each OCR log file should correspond to a particular day of screenshots processed by the OCR rather
# than a OCR log file per day of processing of the OCR).

#extracting the last day executed by the OCR and stored in the BigTable
name_folder = f'{data_path}/raw/BigTable'
df = DeltaTable(name_folder).to_pandas()
last_day_string = max(df['date_of_search'])
last_day_obj = datetime.strptime(last_day_string, '%Y-%m-%d').date()


# path of the screenshots folders
raw_screenshots_path = f'{data_path}/raw/screenshots'
next_date = False
# while we have not find a perfect match between the number of trips in the delta table and the number of folders in the screenshots folder,
# it means that the last day has not been fully processed by the OCR, so we have to go back in time until we find a match,
# which means that for this particular day, all the trips have been processed by the OCR and stored in the BigTable.
while next_date == False:
    # nbr of trip in the delta table for the last day (OCR side)
    last_day_string = last_day_obj.strftime("%Y-%m-%d")
    nb_trip_dt = df[df['date_of_search'] == last_day_string]['trip'].nunique()
    #nb of trip folders in the screenshots folder, corresponding to the last day (Folder side)
    path = f'{raw_screenshots_path}/{last_day_string}'
    nb_trip_folder = len(os.listdir(path))


     #it means that the last day has been fully processed by the OCR, so we can start processing the screenshots folders
     # from the day after the last day.
    if nb_trip_dt == nb_trip_folder:
        next_date = True
     #it means that the last day has not been fully processed, so we will have to process it again.
     # so we verify the day before to be sure it has been fully processed.
    else:
        last_day_obj =last_day_obj - timedelta(days=1)


# Here is the last day that has been fully processed by the OCR for sure.
last_day_string = last_day_obj.strftime("%Y-%m-%d")


# Executing OCR on each folder

list_dates = os.listdir(raw_screenshots_path)
list_dates.remove('.DS_Store')
list_dates.sort()
days_to_extract =[]
for i in list_dates:
    if i > last_day_string:
        days_to_extract.append(i)
print(days_to_extract)
start_time = time.time()
time_per_days = {}
#days_to_extract = ['2025-06-19','2025-06-20','2025-06-21']
#days_to_extract = ['2099-12-12']
total_nb_trip =0
for date in days_to_extract:
        print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
        print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
        print(f"⚙️ Processing date: {date}")
        start_time_bis = time.time()
        final_date = date
        day_folder = f'{raw_screenshots_path}/{date}/'
        trip_list = os.listdir(day_folder)
        total_nb_trip += len(trip_list)
        try:
             trip_list.remove('.DS_Store')
        except:
             pass
        for trip in trip_list:
             print('------------------------------------------------------------------------------')
             print(f'⌛ Folder {date}/{trip} in process by OCR.')
             big_df = ocr_on_folder(f'{raw_screenshots_path}/{date}/{trip}/', trip, date)
             print(f'✅ Folder {date}/{trip} processed by OCR.')
             storing_data(big_df,"/Data/raw/BigTable")
             print(f'✅ Folder {raw_screenshots_path}/{date}/ stored in deltatable.')
        end_time_bis = time.time()
        delta_time = round(end_time_bis - start_time_bis, 2)
        time_per_days[date]=delta_time
        print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
        
        
        

end_time = time.time()
total_duration = round(end_time - start_time, 2)
duration_per_day = time_per_days
print('totale duration : ',round(end_time - start_time, 2))
print('duration par jour : ', time_per_days)


#storing the logs
status_log =  log_path+'/OCR/Scheduling/main_ocr.log'   
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
with open(status_log, "a") as f:
    f.write(f"{timestamp} - {len(days_to_extract)} days - {total_nb_trip} folders processed in - {total_duration} in secondes - last day processed : {final_date}.\n")
    f.write(f"{time_per_days}")

