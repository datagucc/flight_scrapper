select 
cast(date_of_search as date) as hello
from "FPT".big_table

select
*
from "FPT".big_table
where flight_date between '2025-07-01' and '2026-032-30'
order by flight_price desc


select
*
from "FPT".big_table
where flight_price = 'no_price_founded';
where is_error = True;

