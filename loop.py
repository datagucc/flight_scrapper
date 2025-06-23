import csv
import datetime
my_var = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
print(my_var)
data ={'time':my_var}
with open('/Users/focus_profond/GIT_repo/flight_price_tracker/Logs/Pipeline/schedule_scrapping/my_file.txt','w') as txt:
    txt.write(my_var)

print('hello LOOP.py')