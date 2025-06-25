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

# API AUTHORIZATION CONFIG
from configparser import ConfigParser
# Load API credentials from configuration file
parser = ConfigParser()
CONFIGFILE = '/Users/focus_profond/GIT_repo/flight_price_tracker/Config/pipeline.conf'
parser.read(CONFIGFILE)
db_credentials = parser["postgresql_db"]



#We get the max(id) of the big table from the db postgresql to filter our gold layer.
#name_folder = '/Users/focus_profond/GIT_repo/flight_price_tracker/Data/silver/BigTable'
#df_gold = DeltaTable(name_folder).to_pandas()
#maxid = max(df_gold['id'])

#FILTERING THE  CONFIGURATION DB
#connextion to the DB
DB_HOST = db_credentials['DB_HOST']
DB_PORT = db_credentials['DB_PORT']
DB_NAME = db_credentials['DB_NAME']
DB_USER = db_credentials['DB_USER']
DB_PASS = db_credentials['DB_PASS']
DB_SCHEMA = "FPT"  #flight price tracker
DB_TABLE = "big_table"
my_conn = psycopg2.connect(
    database = DB_NAME
	,user= DB_USER
	,password= DB_PASS
	,host= DB_HOST
	,port= DB_PORT
)
cur = my_conn.cursor()
maxid_query = f"""SELECT max(date_of_search) max_date
                      ,max(id) max_id 
	FROM "{DB_SCHEMA}".{DB_TABLE};"""
cur.execute(maxid_query)
records = cur.fetchone()
maxdate = records[0]
maxid = records[1]
print(maxid)
my_conn.commit()
cur.close()
my_conn.close()


#We get the big table from the silver layer
name_folder = '/Users/focus_profond/GIT_repo/flight_price_tracker/Data/silver/BigTable'
df_silver = DeltaTable(name_folder).to_pandas()

#We filter from the id, so the transformation will cost less in computation.
#maxid = 0
if maxid == None:
    maxid = 0
df_silver = df_silver.loc[df_silver['id']> maxid]
df_silver["flight_date"] = pd.to_datetime(df_silver["flight_date"])
df_silver["date_of_search"] = pd.to_datetime(df_silver["date_of_search"])

df_silver["days_before_flight"] = (df_silver["flight_date"] - df_silver["date_of_search"]).dt.days
#df_silver = df_silver.loc[df_silver['date_of_search']> '2025-06-13']

print(len(df_silver))


# Adding the new rows into the postgresql table
my_conn = psycopg2.connect(
    database = DB_NAME
	,user= DB_USER
	,password= DB_PASS
	,host= DB_HOST
	,port= DB_PORT
)
cur = my_conn.cursor()

buffer = StringIO()
df_silver.to_csv(buffer, index=False, header=False)
buffer.seek(0)
copy_query = f"""COPY "{DB_SCHEMA}".{DB_TABLE} ({', '.join(df_silver.columns)}) FROM STDIN WITH CSV"""
cur.copy_expert(copy_query, buffer)
my_conn.commit()
cur.close()
my_conn.close()


#inserting agg_per_date data
agg_per_date_table = "agg_per_date"
default_price = "-1"
default_date = "2099-12-31"
agg_per_date_query = f"""
TRUNCATE TABLE "{DB_SCHEMA}".{agg_per_date_table};
INSERT INTO "{DB_SCHEMA}".{agg_per_date_table} (flight_date, 
												flight_price, 
                                                trip, 
                                                date_of_search, 
                                                id,
                                                days_before_flight, 
                                                avg_price_per_date, 
                                                max_price_per_date, 
                                                min_price_per_date)

SELECT
			flight_date
			,flight_price
			,trip
			,date_of_search
			,id
			,days_before_flight
			,avg(cast(flight_price as numeric)) over (partition by flight_date, trip) avg_price_per_date
			,max(cast(flight_price as numeric)) over (partition by flight_date, trip) max_price_per_date
			,min(cast(flight_price as numeric)) over (partition by flight_date, trip) min_price_per_date
				FROM "{DB_SCHEMA}".{DB_TABLE}
				where flight_price <> {default_price} and flight_date <> '{default_date}'
    ;
"""

my_conn = psycopg2.connect(
    database = DB_NAME
	,user= DB_USER
	,password= DB_PASS
	,host= DB_HOST
	,port= DB_PORT
)

cur = my_conn.cursor()
cur.execute(agg_per_date_query)
my_conn.commit()
cur.close()
my_conn.close()

# inserting the agg_per_trip data
agg_per_trip_table = "agg_per_trip"
agg_per_trip_query = f"""
TRUNCATE TABLE "{DB_SCHEMA}".{agg_per_trip_table};
INSERT INTO "{DB_SCHEMA}".{agg_per_trip_table} (flight_date, 
												flight_price, 
                                                trip, 
                                                date_of_search, 
                                                id,
                                                days_before_flight, 
                                                avg_price_per_trip, 
                                                max_price_per_trip, 
                                                min_price_per_trip)

SELECT
			flight_date
			,flight_price
			,trip
			,date_of_search
			,id
			,days_before_flight
			,avg(cast(flight_price as numeric)) over (partition by flight_date, trip) avg_price_per_trip
			,max(cast(flight_price as numeric)) over (partition by flight_date, trip) max_price_per_trip
			,min(cast(flight_price as numeric)) over (partition by flight_date, trip) min_price_per_trip
				FROM "{DB_SCHEMA}".{DB_TABLE}
				where flight_price <> {default_price} and flight_date <> '{default_date}'
    ;
"""

my_conn = psycopg2.connect(
    database = DB_NAME
	,user= DB_USER
	,password= DB_PASS
	,host= DB_HOST
	,port= DB_PORT
)

cur = my_conn.cursor()
cur.execute(agg_per_trip_query)
my_conn.commit()
cur.close()
my_conn.close()