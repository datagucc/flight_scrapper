print("THE MAIN SCRAPPER SCRIPT BEGINS.")
import pandas as pd
import sys
import openpyxl
import time
import datetime
import logging
import os

# ATTENTION ICI ON TRICHE CAR ON ECRIT LE CHEMIN EN DUR, NORMALEMENT ON DEVRAIT FAIRE CA DIFFERMEENT !!!
root_dir = '/Users/focus_profond/GIT_repo/flight_price_tracker'
if root_dir not in sys.path:
    sys.path.append(root_dir)
from Config.constants import PATH
#root_dir = PATH['main_path']
config_dir = PATH['config_path']
log_path = f"{PATH['logs_path']}/Scrapping/Scheduling"
data_path = PATH['data_path']

import Modules.google_flight_scrapping as google_flight_scrapping


# Open excel file where the URL and trip are stored
excel_path = f'{config_dir}/trip_config.xlsx'
df = pd.read_excel(excel_path, sheet_name='good_one')

# Drop the raw where there is no URL  +  filter to keep only url and trip columns
df= df.dropna(subset=['url'])
df = df[['url', 'trip']]

# Calculating the total amount of time to scrape every trip
start_time = time.time()
timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
print("The main scrapping script started at", timestamp)

for row in df.itertuples():
    print(f"Starting the scrapping of {row.trip}")
    #MAIN FUNCTION
    google_flight_scrapping.scrapping_url(row.url, row.trip)
    print('-----------------------------------------------')
end_time = time.time()
total_duration = round(end_time - start_time, 2)

#storing the logs
status_log =  log_path+'/main_scrapper.log'   
timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
date_concerned = datetime.datetime.now().strftime("%Y-%m-%d")
raw_screenshots_path = f'{data_path}/raw/screenshots/{date_concerned}' 
try :
    list_dates = os.listdir(raw_screenshots_path)
except:
    list_dates = []
nb_trip = len(list_dates)
with open(status_log, "a") as f:
    f.write(f"{timestamp} - {nb_trip} trips - {total_duration} en secondes.\n")
    f.write('-----------------------------------------------')
