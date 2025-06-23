SELECT
flight_date
,flight_price
,trip
, date_of_search
,id
, days_before_flight
--,(cast(flight_date as date) - cast(date_of_search as date)) as days_before_flight
,avg(cast(flight_price as numeric)) over (partition by flight_date, trip) avg_price_per_date
,max(cast(flight_price as numeric)) over (partition by flight_date, trip) max_price_per_date
,min(cast(flight_price as numeric)) over (partition by flight_date, trip) min_price_per_date
,avg(cast(flight_price as numeric)) over (partition by  trip) avg_price_per_trip
,max(cast(flight_price as numeric)) over (partition by  trip) max_price_per_trip
,min(cast(flight_price as numeric)) over (partition by  trip) min_price_per_trip
	FROM "FPT".big_table
	where flight_price <> -1 and flight_date <> '2099-12-31'
	order by days_before_flight asc;