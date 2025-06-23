print("HELLO THE MAIN SCRAPPER SCRIPT BEGINS.")
import pandas as pd
import sys
import os
import openpyxl
import time
#CALL OF MY MODULE
root_dir = '/Users/focus_profond/GIT_repo/flight_price_tracker'
if root_dir not in sys.path:
    sys.path.append(root_dir)
import Scripts.scrapper.modules.scrapper as scrapper



# Ouvre le fichier Excel
# FULL PATH NOT RELATIVE PATH
df = pd.read_excel("/Users/focus_profond/GIT_repo/flight_price_tracker/Config/trip_config.xlsx", sheet_name='good_one')

# Affiche les 5 premières lignes
df= df.dropna(subset=['url'])
#select only the columns we need
df = df[['url', 'trip']]


start_time = time.time()
for row in df.itertuples():
    print(f"Starting the scrapping of {row.trip}")
    #MAIN FUNCTION
    scrapper.scrapping_url(row.url, row.trip)
end_time = time.time()
print('totale duration : ',round(end_time - start_time, 2))