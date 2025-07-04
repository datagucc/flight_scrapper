import sys
#import os
#import time
project_path = '/Users/focus_profond/GIT_repo/flight_price_tracker'
if project_path not in sys.path:
    sys.path.append(project_path)
#Importing personal modules
from Config.constants import PATH
from Modules.metadata_functions import *
from Modules.DF_functions import *
from Modules.postgresql_utils import *
data_path = PATH['data_path']
log_path = PATH['logs_path']



def main():

	# CONNECTION TO THE DB
	cur, my_conn = connection_to_postgresql()

	#Getting the max(id) of the big table of the postgresql database
	DB_SCHEMA = "FPT"  #flight price tracker
	DB_TABLE = "big_table"
	maxid_query = f"""SELECT max(date_of_search) max_date
						,max(id) max_id 
		FROM {DB_SCHEMA}.{DB_TABLE};"""
    
	try:
		records = request_query(maxid_query,cur,type='fetchone')
		maxdate = records[0]
		maxid_db = records[1]
	except:
		maxdate='1900-01-01'
		maxid_db = 0
	
	#getting the max(id) of the big table from the gold layer
	#name_folder = f"{data_path}/gold/BigTable"   
	#df_gold = DeltaTable(name_folder).to_pandas()
	#maxid_gold = max(df_gold['id']) if not df_gold.empty else 0 
	#nb_of_rows_gold = df_gold.shape[0]


	#We get the big table from the silver layer
	name_folder = f"{data_path}/silver/BigTable" 
	df_silver = DeltaTable(name_folder).to_pandas()
	nb_of_rows_silver = df_silver.shape[0]

	#We filter from the id, so the transformation will cost less in computation.
	#maxid = 0
	if maxid_db == None:
		maxid_db = 0
	df_silver = df_silver.loc[df_silver['id']> maxid_db]
	
	df_silver["flight_date"] = pd.to_datetime(df_silver["flight_date"])
	df_silver["date_of_search"] = pd.to_datetime(df_silver["date_of_search"])
	df_silver["days_before_flight"] = (df_silver["flight_date"] - df_silver["date_of_search"]).dt.days
	#df_silver = df_silver.loc[df_silver['date_of_search']> '2025-06-13']
    
	#filtering the bad data :   
	df_silver = df_silver.loc[df_silver['flight_price'] != '-1']
	df_silver = df_silver.loc[df_silver['flight_date'] != '2099-12-31'] 
	nb_of_new_rows = len(df_silver) 
      
	# inserting the new rows into the gold deltatable  --> it does not work but it is not useful anyway
	#name_folder = f"{data_path}/gold/BigTable"  
	#partition_cols = None
	#predicate = "target.id = source.id"
	#source = 'silver_layer'
	#author = 'Augustin'
	#save_new_data_as_delta(df_silver,name_folder,predicate= predicate, partition_cols=partition_cols, layer = 'Gold', source= source, author =author)



	# Adding the new rows into the postgresql table
	# INSERTING ROW INTO THE FACT TABLE
	try:
		copying_data(df_silver,DB_TABLE,cur,my_conn,db_schema=DB_SCHEMA)
	except Exception as e:
		print("An error occured while cpoying the data into the postgresql table.",e)
	#counting the number of rows after the insertion :
	count_query = f"""SELECT count(*) 
		FROM {DB_SCHEMA}.{DB_TABLE};"""
	records = request_query(count_query,cur,type='fetchone')
	nb_of_rows_gold = records[0]
    


	#INSERTING ROW IN THE DIM DATE TABLE
	first_date = '2010-01-01'
	last_date = '2050-01-01'

	def create_dim_date(first_date, last_date):
		"""
		Create a DataFrame with a date range from first_date to last_date.

		Args = first_date (str) : Dates in 'YYYY-MM-DD' format

		"""
		date_range = pd.date_range(start=first_date, end=last_date, freq='D')
		df = pd.DataFrame(date_range, columns=['date'])
		
		# Add additional columns
		df['year'] = df['date'].dt.year
		df['month'] = df['date'].dt.month
		df['month_name'] = df['date'].dt.month_name()
		df['quarter'] = df['date'].dt.quarter
		df['day'] = df['date'].dt.day
		df['day_name'] = df['date'].dt.day_name()
		df['day_of_week'] = df['date'].dt.dayofweek
		df['week_of_year'] = df['date'].dt.isocalendar().week
		df['is_weekend']=df['date'].dt.dayofweek >=5
		#df['hemisphere_nord'] = ''
		#df['hemisphere_sud'] = ''
		#df['hemisphere_central'] = ''
		
		return df
	df_dim_date = create_dim_date(first_date,last_date)
	#df = create_dim_date(first_date,last_date)
	csv_path = '/Users/focus_profond/GIT_repo/flight_price_tracker/Data/dim_files/all_dim.xlsx'
	df_dim_season = pd.read_excel(csv_path, sheet_name='dim_season')
	df_dim_date = df_dim_date.merge(df_dim_season, left_on='month_name',right_on='month', how="left")
	df_dim_date= df_dim_date.rename(columns={'month_x':'month'})
	df_dim_date= df_dim_date.drop(columns=['month_y'])

	#df_dim_date = create_dim_date(first_date,last_date)
	try:
		copying_data(df_dim_date,'dim_date',cur,my_conn,db_schema=DB_SCHEMA)
	except Exception as e:
		print("An error occured while cpoying the data into the postgresql table.",e)


     #INSERTING THE DIM CURRENCY DATA
	 # #API AUTHORIZATION CONFIG
	from configparser import ConfigParser
	parser = ConfigParser()
	CONFIGFILE = '/Users/focus_profond/GIT_repo/flight_price_tracker/Config/pipeline.conf'
	parser.read(CONFIGFILE)
	# Freecurrency API configuration
	api_credentials = parser["freecurrency_api"]
	access_token = api_credentials["access_token"]
	base_url = api_credentials["base_url"]

	#endpoint = lastest
	base_currency = 'EUR'
	currencies = 'EUR,USD,CAD'
	params ={
			'apikey':access_token
		# ,'currencies':currencies
			,'base_currency':base_currency
		}
	headers={}
	endpoint = "latest"
	json_data = get_data(base_url, endpoint, data_field='data', params = params,headers=headers)

	#building the DF 
	dict = json_data[0]
	lastest_rates = pd.DataFrame.from_dict(dict,orient='index')
	lastest_rates['base_currency'] = base_currency
	lastest_rates = lastest_rates.reset_index()
	lastest_rates = lastest_rates.rename(columns={0:'exchange_rate','index':'currencies'})
	lastest_rates['day_of_rates'] = datetime.now().strftime('%Y-%m-%d')
	#inserting 
	try:
		copying_data(lastest_rates,'dim_currency',cur,my_conn,db_schema=DB_SCHEMA)
	except Exception as e:
		print("An error occured while cpoying the data into the postgresql table.",e)

	



	#inserting agg_per_date data
	agg_per_date_table = "agg_per_date"
	default_price = "-1"
	default_date = "2099-12-31"

	agg_per_date_query = f"""
	TRUNCATE TABLE {DB_SCHEMA}.{agg_per_date_table};
	INSERT INTO {DB_SCHEMA}.{agg_per_date_table} (flight_date, 
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
					FROM {DB_SCHEMA}.{DB_TABLE}
					where flight_price <> {default_price} and flight_date <> '{default_date}'
		;
	"""
	execute_query(agg_per_date_query,cur,my_conn, query_name='agg_per_date_query')

	# inserting the agg_per_trip data
	agg_per_trip_table = "agg_per_trip"
	agg_per_trip_query = f"""
	TRUNCATE TABLE {DB_SCHEMA}.{agg_per_trip_table};
	INSERT INTO {DB_SCHEMA}.{agg_per_trip_table} (flight_date, 
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
					FROM {DB_SCHEMA}.{DB_TABLE}
					where flight_price <> {default_price} and flight_date <> '{default_date}'
		;
	"""

	execute_query(agg_per_trip_query,cur,my_conn, query_name='agg_per_trip_query')



	#closing the connection :
	closing_connection(my_conn,cur)
    
	#print('----------------------------------------------')
	#print('--------- METADATA OF GOLD LAYER --------------')
	#print(f"Size of the silver table : {nb_of_rows_silver} rows.") 
	#print(f"Size of the gold table : {nb_of_rows_gold} rows.") 
	#print(f"Nb of new rows  : {nb_of_new_rows}.") 
    
	return (f"""
            ----------------------------------------------\n
            --------- METADATA OF GOLD LAYER --------------\n
            Size of the silver table : {nb_of_rows_silver} rows.\n
            Max id of the postgresql table before the insertion: {maxid_db}.\n
            Nb of new rows  : {nb_of_new_rows}.\n
            Size of the gold table after the insertion: {nb_of_rows_gold} rows.\n
            
""")



if __name__ == "__main__":
    main()