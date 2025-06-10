import pandas as pd
import scrapper as scrapper
import openpyxl
import time

# Ouvre le fichier Excel
# FULL PATH NOT RELATIVE PATH
df = pd.read_excel("/Users/focus_profond/GIT_repo/flight_price_tracker/Config/trip_config.xlsx", sheet_name='good_one')

# Affiche les 5 premières lignes
df= df.dropna(subset=['url'])
#select only the columns we need
df = df[['url', 'trip']]

#my_log =scrapper.scrapping_url('https://www.google.com/travel/flights?tfs=CBwQARoXagcIARIDQlJVcgwIAxIIL20vMDEydHNAAUgBcAGCAQsI____________AZgBAg&tfu=KgIIAw&curr=EUR','BRU_AKL', 5)
start_time = time.time()
for row in df.itertuples():
    print(f"Starting the scrapping of {row.trip}")
    scrapper.scrapping_url(row.url, row.trip)
end_time = time.time()
print('totale duration : ',round(end_time - start_time, 2))

    
#print(my_log)
