#Import libraries
import sys
import os
import shutil
import time
import pandas as pd
#test OCR
import pytesseract
import cv2
from PIL import Image
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
import calendar
from deltalake import write_deltalake, DeltaTable
from deltalake.table import TableOptimizer
from deltalake.exceptions import TableNotFoundError
import re
import logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from Modules.DF_functions import *





#On renomme les captures d'écran de manière dynamique

def rename_files_by_month(source_folder_path, destination_folder_path='/Users/focus_profond/GIT_repo/flight_price_tracker/Data_sources/renammed/'):
    # Liste et trie les fichiers alphabétiquement
    files = sorted([f for f in os.listdir(source_folder_path) if os.path.isfile(os.path.join(source_folder_path, f))])

    #folder to store
    #new_folder= destination_folder_path
    
    count = 0
    for file in files:
        # Extension
        ext = os.path.splitext(file)[1]

        #on extrait la date de la recherche
        match = re.search(r'(\d{4}-\d{2}-\d{2})', file)
        if match:
            date_of_search = match.group(1)
        #new_folder_src = '/Users/focus_profond/GIT_repo/flight_price_tracker/Data_sources/renammed/'
        #new_folder = new_folder_src+'/'+date_of_search

        # Obtenir le nom du dossier
        list_name = source_folder_path.split('/')
        name_folder = list_name[len(list_name)-1]

        #obtenir le mois et l'année de départ
        date_obj =datetime.strptime(date_of_search, "%Y-%m-%d")
        date_obj += relativedelta(months=count)
        month_year = date_obj.strftime("%B_%Y").lower()


        # Nouveau nom de fichier
        new_name = f"{date_of_search}_XxX_{name_folder}_XxX_{month_year}{ext}"
        
        # Chemins complet
        src = os.path.join(source_folder_path, file)
        dst = os.path.join(destination_folder_path, new_name)
        
        print(f"Copie de {file} -> {new_name}")
        #os.rename(src, dst)
        shutil.copy2(src, dst)
        # Mois suivant
        count+=1



# On pratique l'OCR sur un fichier individuel
"""
def ocr_individual_cells(image_path, image_name,rows=6, cols=7, config='--psm 6'):
    full_path = image_path+image_name
    file_name = image_name.split('.png')[0]
    month_flight = file_name.split('_XxX_')[2]
    trip = file_name.split('_XxX_')[1]
    date_of_search = file_name.split('_XxX_')[0]
    img = cv2.imread(full_path)
    height, width, _ = img.shape

    cell_h = height // rows
    cell_w = width // cols

    results = {}

    for r in range(rows):
        for c in range(cols):
            x1 = c * cell_w
            y1 = r * cell_h
            x2 = (c + 1) * cell_w
            y2 = (r + 1) * cell_h

            cell_img = img[y1:y2, x1:x2]

            # OCR sur la cellule seule
            text = pytesseract.image_to_string(cell_img, config=config).strip()
            if text != '':
                
                separateur = text.split('\n')
                
                day_flight = separateur[0]+'_'+month_flight
                if len(separateur)>1:
                    price = separateur[1]#.replace("€",'')
                else:
                    price= 'NULL'
                results[day_flight] = price
    
    my_df = pd.json_normalize(results)
    my_df = my_df.transpose()
    my_df =my_df.reset_index().rename(columns={'index':'flight_date',0:'flight_price'})
    my_df['date_of_search'] = date_of_search
    my_df['trip'] = trip
    my_df['file_name'] = image_name
    my_df['flight_date'] = pd.to_datetime(my_df['flight_date'], format="%d_%B_%Y").dt.strftime("%Y-%m-%d")
    #je dois rajouter la colonne "trip" + la colonne "name_file" + la colonne "date_of_search"
    #on sép

    return my_df


"""

# Configuration basique du log (fichier, format, niveau)
logging.basicConfig(filename='/Users/focus_profond/GIT_repo/flight_price_tracker/Logs/ocr_errors.log',
                    filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.ERROR)

def ocr_individual_cells_errors(image_path, image_name, rows=5, cols=7, config='--psm 6'):
    errors = []
    is_error = False

    try:
        full_path = os.path.join(image_path, image_name)

        # Vérification des parties dans le nom du fichier
        file_name = image_name.split('.png')[0]
        parts = file_name.split('_XxX_')
        if len(parts) < 3:
            logging.error(f"Nom de fichier inattendu (manque de parties séparées par _XxX_) : {image_name}")
            return pd.DataFrame()  # Pas de DataFrame avec erreurs, juste log

        date_of_search, trip, month_flight = parts[0], parts[1], parts[2]

        # Lecture de l'image
        img = cv2.imread(full_path)
        if img is None:
            logging.error(f"Impossible de lire l'image : {full_path}")
            return pd.DataFrame()  # Pas de DataFrame avec erreurs, juste log
        
        height, width, _ = img.shape
        if height == 640:
            rows =6
        cell_h = height // rows
        cell_w = width // cols

        results = {}

        for r in range(rows):
            for c in range(cols):
                try:
                    x1 = c * cell_w
                    y1 = r * cell_h
                    x2 = (c + 1) * cell_w
                    y2 = (r + 1) * cell_h

                    cell_img = img[y1:y2, x1:x2]
                    text = pytesseract.image_to_string(cell_img, config=config).strip()

                    if text != '':
                        separateur = text.split('\n')
                        day_flight = separateur[0] + '_' + month_flight
                        price = separateur[1] if len(separateur) > 1 else 'no_price_founded'
                        results[day_flight] = price
                except Exception as e:
                    # On log l’erreur OCR sur la cellule mais on continue
                    logging.error(f"Erreur OCR cellule ({r},{c}) dans {image_name} : {e}")
                    errors.append(f"OCR cellule ({r},{c}): {str(e)}")

        # Construction du DataFrame
        try:
            my_df = pd.json_normalize(results).transpose().reset_index().rename(columns={'index': 'flight_date', 0: 'flight_price'})
            date_of_search = date_of_search or 'no_date_of_search_founded'
            trip = trip or "no_trip_founded"
            image_name = image_name or "no_image_name_founded"
            my_df['date_of_search'] = date_of_search
            my_df['trip'] = trip
            my_df['file_name'] = image_name

            try:
                my_df['flight_date'] = pd.to_datetime(my_df['flight_date'], format="%d_%m_%Y", errors='coerce')
                if my_df['flight_date'].isna().any():
                    warning_msg = f"Dates non converties dans {image_name}"
                    logging.warning(warning_msg)
                    errors.append(warning_msg)
                my_df['flight_date'] = my_df['flight_date'].dt.strftime("%Y-%m-%d")
            except Exception as e:
                err_msg = f"Erreur conversion dates dans {image_name} : {e}"
                logging.error(err_msg)
                errors.append(err_msg)
                my_df['flight_date'] = my_df['flight_date'].fillna('no_flightdate_founded')

        except Exception as e:
            err_msg = f"Erreur création DataFrame dans {image_name} : {e}"
            logging.error(err_msg)
            errors.append(err_msg)
            # Crée un df avec juste les infos minimales
            my_df = pd.DataFrame({'date_of_search': [date_of_search], 'trip': [trip], 'flight_date': [None], 'flight_price': [None]})

        # Ajout des colonnes is_error et errors dans le DataFrame
        if errors:
            is_error = True
        my_df['is_error'] = is_error
       # my_df['errors'] = [errors] * len(my_df)

        return my_df

    except Exception as e:
        # Erreur inattendue globale, log et return DataFrame vide
        logging.error(f"Erreur inattendue dans OCR fichier {image_name} : {e}")
        return pd.DataFrame()





# OCR sur un dossier entier

def ocr_on_folder(folder_path):
    names = os.listdir(folder_path)
    big_df = pd.DataFrame({})
    print(f'starting loading folder : {folder_path}')
    for name in names:
        print('starting the loading of the  the file :',name)
        df = ocr_individual_cells_errors(folder_path,name)
        big_df = pd.concat([big_df,df], ignore_index=True, sort=False)
       # print('ending of the loading of the file')
    print(f'ending loading folder : {folder_path}')
    #print(f'longueur du df : {len(big_df)}')
    return big_df


#stocker le DF dans un fichier DeltaLake :
#améliorer cette fonction pour qu'elle soit plus dynamique 
#def storing_data(df, directory='/Users/focus_profond/GIT_repo/IDLE_CITY_PROJECT/Accessibility', source = 'screenshots', author = 'Augustin'):
def storing_data(df, name_folder_desti='/Data/bronze/BigTable', source = 'screenshots', author = 'Augustin'):
    
    main_directory = '/Users/focus_profond/GIT_repo/flight_price_tracker'
    os.chdir(main_directory)
    name_folder = main_directory+ name_folder_desti
    partition_cols = None
    predicate = "target.flight_date = source.flight_date AND target.trip = source.trip AND target.date_of_search = source.date_of_search"

    save_new_data_as_delta(df,name_folder,predicate= predicate, partition_cols=partition_cols, layer = 'Bronze', source= source, author =author)
