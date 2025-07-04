import pandas as pd
import pycountry
import pycountry_convert
import geopy

def create_dim_date(first_date, last_date):


            try: 
            
                my_date_range = pd.date_range(start=first_date, end=last_date, freq='D')
                df = pd.DataFrame(my_date_range, columns=['date'])
                
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
			
            except Exception as e:
                print('error',str(e))
		
            return df
#first_date = '2010-01-01'
#last_date = '2050-01-01'
#df= create_dim_date(first_date,last_date)


def country_to_continent(country_name):
    try:
        # Trouve l'objet pycountry (tolère les imprécisions)
        country = pycountry.countries.get(name=country_name)
        if not country:
            country = pycountry.countries.search_fuzzy(country_name)[0]
        code_alpha2 = country.alpha_2

        # Convertit code pays en code continent, puis nom complet
        code_continent = pycountry_convert.country_alpha2_to_continent_code(code_alpha2)
        nom_continent = pycountry_convert.convert_continent_code_to_continent_name(code_continent)
        return nom_continent

    except Exception as e:
        return f"Erreur pour {country_name} : {e}"
    



def get_hemisphere_from_country(country_name):
    try:
        geolocator = geopy.Nominatim(user_agent="hemisphere-lookup")
        location = geolocator.geocode(country_name, language='en')
        if location is None:
            return "Inconnu"
        
        latitude = location.latitude
        if latitude > 10:
            return "Nord"
        elif latitude < -10:
            return "Sud"
        else:
            return "Central"

    except Exception as e:
        return f"Erreur : {e}"

def get_type_hault(distance_km):
    if distance_km < 1500:
        return 'short_haul'
    elif distance_km < 3500:
        return 'medium_haul'
    elif distance_km < 6000:
        return 'long_haul'
    else:
        return 'ultra_long_haul'
    

