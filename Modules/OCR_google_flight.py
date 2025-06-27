#Import libraries
import sys
import os
import shutil
#import time
import pandas as pd
import pytesseract
import cv2
import csv
import time
#from PIL import Image
#import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
#import calendar
from deltalake import write_deltalake, DeltaTable
from deltalake.table import TableOptimizer
from deltalake.exceptions import TableNotFoundError
import re
import logging
from Config.constants import PATH
root_dir = PATH['main_path']
log_path = PATH['logs_path']
if root_dir not in sys.path:
    sys.path.append(root_dir)
#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from Modules.DF_functions import *

"""
#Pour voir quel pytesseract est utilisé :
#  Pytesseract est un wrapper pour Tesseract OCR, qui est un moteur de reconnaissance optique de caractères (OCR) open source.
# Cela signifie que Pytesseract fait appel à tesseract en arrière plan pour effectuer l'OCR. 
# De son coté, tesseract est un logiciel indépendant qui doit etre installé sur mon systeme (via Brew par exemple).
# Et donc Tesseract n'est pas un module Python mais un programme externe que le module Pytesseract appelle.
#import pytesseract
#print("Tesseract utilisé :", pytesseract.pytesseract.tesseract_cmd)
# Erreur que j'ai eu : Tesseract is not installed or it's not in your PATH
# On sait que pytesseract est installé (effectivement lorsqu'on fait pip show pytesseract quand on est dans l'env virtuel, on voit qu'il est installé)
# Donc cela veut dire que l'erreur est lié au binaire TESSERACT. 
# On sait que Tesseract est installé donc cela veut dire que l'erreur vient du path : il n'est pas trouvé par pytesseract.
# Pour corriger cela, on doit spécifier le chemin du binaire Tesseract + #forcing manually the path to Tesseract
os.environ["LANG"] = "en_US.UTF-8"
os.environ["LC_ALL"] = "en_US.UTF-8"
pytesseract.pytesseract.tesseract_cmd = "/opt/homebrew/bin/tesseract"
os.environ["PATH"] += os.pathsep + "/opt/homebrew/bin"
import subprocess
tess_path = pytesseract.pytesseract.tesseract_cmd
# Debug info
with open("/Users/focus_profond/GIT_repo/flight_price_tracker/Logs/OCR/Scheduling/ocr_debug.log", "a") as f:
    f.write("==== NOUVELLE EXECUTION ====\n")
    f.write(f"TESSERACT_CMD: {pytesseract.pytesseract.tesseract_cmd}\n")
    f.write(f"PATH: {os.environ['PATH']}\n")
        # Test subprocess brut
    try:
        version_output = subprocess.check_output([tess_path, "--version"], stderr=subprocess.STDOUT)
        f.write("✅ Subprocess fonctionne ! Version :\n")
        f.write(version_output.decode("utf-8"))
    except Exception as e:
        f.write(f"❌ Subprocess échoue : {str(e)}\n")
"""
#MALGRE TOUTES LES PRECAUTIONS, IL SEMBLE QUE J AI TOUJOURS L ERREUR. IL SEMBLERAIT QUE CA VIENNE DE LA GESITON DES FICHIERS TEMPORAIRES PAR PYTESSERACT.
# POUR CELA, JE VAIS MOI-MEME GERER LES FICHIERS TEMPORAIRES
pytesseract.pytesseract.tesseract_cmd = "/opt/homebrew/bin/tesseract"
os.environ["PATH"] += os.pathsep + "/opt/homebrew/bin"

# 1) Create a temporary directory
TEMP_OCR_DIR = "/Users/focus_profond/GIT_repo/flight_price_tracker/Data/temp/tmp_ocr"
os.makedirs(TEMP_OCR_DIR, exist_ok=True)

# 2) Create a personnalised function to replace pytesseract.image_to_string
import uuid
import subprocess
import cv2

def custom_image_to_string(cell_img, config='', lang='eng'):
    # Génère un nom unique
    unique_id = str(uuid.uuid4())
    input_path = os.path.join(TEMP_OCR_DIR, f"{unique_id}.png")
    output_base = os.path.join(TEMP_OCR_DIR, f"{unique_id}")
    output_txt = f"{output_base}.txt"

    # Sauvegarde l’image
    cv2.imwrite(input_path, cell_img)

    try:
        # Appel à tesseract
        cmd = [
            pytesseract.pytesseract.tesseract_cmd,
            input_path,
            output_base,
            '--psm', '6',
            '-l', lang
        ]
        if config:
            cmd += config.split()

        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Lecture du résultat
        if os.path.exists(output_txt):
            with open(output_txt, 'r', encoding='utf-8') as f:
                return f.read().strip()
        else:
            return ""

    finally:
        # Nettoyage des fichiers temporaires
        for path in [input_path, output_txt]:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass
# ensuite je peux simplement remplacer l'appel de pytsseract par ma fonction : text = custom_image_to_string(cell_img, config=config)

# ANALYSIS OF EXECUTION TIME BEWTEEN THE 2 SOLUTIONS, with one date and 2 trips (so 2 folders)
# using my own function, running on VSCode : 59.31
# using my own function, running on cron : 112 secondes
# using pytesseract.image_to_string, running on VSCode : 59.47
# using pytesseract.image_to_string, running on cront :


def ocr_individual_cells_errors(image_path, image_name, rows=5, cols=7, config='--psm 6'):
    """
    Perform OCR on a grid-based flight price screenshot and extract prices per date.

    The function assumes the image is divided into a fixed number of rows and columns,
    corresponding to a calendar grid (e.g., 5 rows x 7 columns). It uses Tesseract to extract
    text from each cell and builds a DataFrame containing flight dates and prices.

    Args:
        image_path (str): Directory path where the image is located.
        image_name (str): Filename of the image to process (expected format: date_XxX_trip_XxX_month.png).
        rows (int, optional): Number of calendar rows. Adjusted to 6 if image height == 640. Default is 5.
        cols (int, optional): Number of calendar columns. Default is 7.
        config (str, optional): Tesseract configuration string. Default is '--psm 6'.

    Returns:
        pd.DataFrame: A DataFrame with the following columns:
            - flight_date (str, formatted as 'YYYY-MM-DD', or 'no_flightdate_founded' on failure)
            - flight_price (str, price string or 'no_price_founded')
            - date_of_search (str, extracted from filename or fallback)
            - trip (str, extracted from filename or fallback)
            - file_name (str, the image name or fallback)
            - is_error (bool, True if any error occurred during OCR or processing)

    Notes:
        - If the image name format is invalid, or the image cannot be read, returns an empty DataFrame.
        - Logs all OCR and processing errors but continues processing when possible.
        - If some dates can't be parsed, they are replaced with a default placeholder.
    
    Cette fonction va retourner un dataframe qui reprend tout les prix pour chaque date. Si tout se passe bien, pas d'erreurs, si certains trucs se passent pas bien
    il y aura des erreurs levés. L'idée, c'est que les erreurs levés soient retournées pour qu'elles puissent être utilisés dans la fonction full dossier.
    Toutes les erreurs seront stockées dans le dictionnaire "errors"
    """
    #errors = []
    errors = {}
    is_error = False


    try:
        full_path = os.path.join(image_path, image_name)

        # Check if the file has the correct naming convention
        file_name = image_name.split('.png')[0]
        parts = file_name.split('_XxX_')
        if len(parts) != 3:
            #logging.error(f"File name have not the correct naming convention : {image_name}")
            err = f"File name have not the correct naming convention : {image_name}"
            errors['file_name_error']= err
            return (pd.DataFrame(), errors)  # We return an empty dataframe as-well as the error dictionnary

        date_of_search, trip, month_flight = parts[0], parts[1], parts[2]

        # Reading the image
        img = cv2.imread(full_path)
        if img is None:
            #logging.error(f"Impossible de lire l'image : {full_path}")
            err = f"Impossible to read image : {full_path}"
            errors['image_reading_error']= err
            return (pd.DataFrame(), errors)   # We return an empty dataframe as-well as the error dictionnary
        
        height, width, _ = img.shape
        # depending on the number of weeks of a month, the screenshots will have different size.
        if height == 640:
            rows =6
        cell_h = height // rows
        cell_w = width // cols

        results = {}
        errors['cell_errors']=[]
        for r in range(rows):
            for c in range(cols):
                try:
                    x1 = c * cell_w
                    y1 = r * cell_h
                    x2 = (c + 1) * cell_w
                    y2 = (r + 1) * cell_h

                    cell_img = img[y1:y2, x1:x2]
                    #text = pytesseract.image_to_string(cell_img, config=config).strip()
                    text = custom_image_to_string(cell_img, config=config)

                    #print("TYPE OF TEXT : ",type(text), " ; TEXT : ",text)

                    if text != '':
                        separateur = text.split('\n')
                        day_flight = separateur[0] + '_' + month_flight
                        price = separateur[1] if len(separateur) > 1 else 'no_price_founded'
                        results[day_flight] = price
                except Exception as e:
                    # On log l’erreur OCR sur la cellule mais on continue
                    # We log the error of the OCR on the cell but we continue 
                    #logging.error(f"Erreur OCR cellule ({r},{c}) dans {image_name} : {e}")
                    #errors.append(f"OCR cellule ({r},{c}): {str(e)}")
                    errors['cell_errors'].append(f"Cell OCR ({r},{c}) : {str(e)}.")
                    

        # Building the dataframe
        errors['dataframe']=[]
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
                    warning_msg = f"Dates non converted in {image_name}"
                    #logging.warning(warning_msg)
                    #errors.append(warning_msg)
                    errors['dataframe'].append(warning_msg)
                my_df['flight_date'] = my_df['flight_date'].dt.strftime("%Y-%m-%d")
            except Exception as e:
                err_msg = f"Dates conversion error in {image_name} : {e}"
                #logging.error(err_msg)
                #errors.append(err_msg)
                errors['dataframe'].append(err_msg)
                my_df['flight_date'] = my_df['flight_date'].fillna('no_flightdate_founded')

        except Exception as e:
            err_msg = f"Dataframe creation error in {image_name} : {e}"
            #logging.error(err_msg)
            #errors.append(err_msg)
            errors['dataframe'].append(err_msg)
            # Creation of a dataframe with the minimal information
            my_df = pd.DataFrame({'date_of_search': [date_of_search], 'trip': [trip], 'flight_date': [None], 'flight_price': [None]})

        # Ajout des colonnes is_error et errors dans le DataFrame
        if errors['dataframe']:
            is_error = True
        my_df['is_error'] = is_error
       # my_df['errors'] = [errors] * len(my_df)
        if errors['cell_errors']:
            is_error = True
        
        if is_error == False:
            errors={}

        #print(my_df.info())
        return (my_df,errors)

    except Exception as e:
        # Erreur inattendue globale, log et return DataFrame vide
        #logging.error(f"Erreur inattendue dans OCR fichier {image_name} : {e}")
        err = f"Unexcepted error during the OCR of the file {image_name} : {e}"
        errors['overall_error']=err
        return (pd.DataFrame(),errors)

# Here are the potential errors from the OCR of a file.
# errors = {file_name_error, image_reading_errors,[cell_errors],[dataframe_error], overall_error}


# OCR sur un dossier entier

def ocr_on_folder(folder_path, trip, date):
    """ Perform OCR on all images in a specified folder and log the results.
    Args:
        folder_path (str): Path to the folder containing images.
        trip (str): Trip identifier.
        date (str): Date of the trip in 'YYYY-MM-DD' format.
    Returns:
        pd.DataFrame: A DataFrame containing the OCR results for all images in the folder.
    """

    date_obj = datetime.now()
    current_date_full = date_obj.strftime("%Y-%m-%d")
    start_time = time.time()
    log_path_a = f"{PATH['logs_path']}/OCR/"
    log_path_b = f"{log_path_a}/Execution_logs/ocr_log_{current_date_full}.csv"
    
    
    names = os.listdir(folder_path)
    nb_of_month = len(names)
    log_data = {
        "trip": trip,
        "date": date,
        "start_time": datetime.now().isoformat(),
        "end_time": None,
        "duration_sec": None,
        "total_months": nb_of_month,
        "errors": [],
        "status": "started",
        "path": folder_path
    }




    big_df = pd.DataFrame({})
    print(f'📁 Starting OCR on folder : {folder_path}')
    has_errors = False
    for name in names:
        print('Starting the OCR of the file :',name)
        # first we want the df of the OCR results and second, we want the dictionnary of the errors.
        df, dict_errors = ocr_individual_cells_errors(folder_path,name)
        msg = 'with no errors.'
        if dict_errors:
            log_data["errors"].append({
                    "file_name": name,
                    "dict_error": dict_errors
                })
            msg= "with errors. ⚠️"
            has_errors = True

        big_df = pd.concat([big_df,df], ignore_index=True, sort=False)
       # print('ending of the loading of the file')
    print(f'Ending OCR on folder : {folder_path}, {msg}')
    if has_errors:
        log_data["status"] = "failure"
    else :
        log_data["status"] = "success"
    #print(f'longueur du df : {len(big_df)}')

    #storing the logs
    end_time = time.time()
    log_data["end_time"] = datetime.now().isoformat()
    log_data["duration_sec"] = round(end_time - start_time, 2)
    errors_str = json.dumps(log_data["errors"], ensure_ascii=False) if log_data["errors"] else "[]"
    with open(log_path_b, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if f.tell() == 0:  # Write header if file is new
                writer.writerow([
                    "trip", "date_scrapped", "start_time", "end_time",
                    "duration_sec", "total_months",
                    "status", "errors_count", "errors", "url"
                ])
            writer.writerow([
                log_data["trip"],
                log_data["date"],
                log_data["start_time"],
                log_data["end_time"],
                log_data["duration_sec"],
                log_data["total_months"],
                log_data["status"],
                len(log_data["errors"]),
                errors_str,
                log_data["path"]
            ])
    print(f"Log saved in {log_path}")




    return big_df


#stocker le DF dans un fichier DeltaLake :
#améliorer cette fonction pour qu'elle soit plus dynamique 
#def storing_data(df, directory='/Users/focus_profond/GIT_repo/IDLE_CITY_PROJECT/Accessibility', source = 'screenshots', author = 'Augustin'):
def storing_data(df, name_folder_desti="/Data/raw/BigTable", source = 'screenshots', author = 'Augustin'):
    """
    Store the DataFrame in a Delta Lake format in the specified directory.

    Args:
        df (pd.DataFrame): The DataFrame to store.
        name_folder_desti (str): The destination folder path where the Delta Lake table will be saved.
        source (str): The source of the data (e.g., 'screenshots'). 
        author (str): The author of the data (e.g., 'Augustin').
    Returns:
        None
    """
    #main_directory = '/Users/focus_profond/GIT_repo/flight_price_tracker'
    main_directory=root_dir
    os.chdir(main_directory)
    name_folder = main_directory+ name_folder_desti
    #print(name_folder)
    partition_cols = None
    predicate = "target.flight_date = source.flight_date AND target.trip = source.trip AND target.date_of_search = source.date_of_search"

    save_new_data_as_delta(df,name_folder,predicate= predicate, partition_cols=partition_cols, layer = 'Bronze', source= source, author =author)




# ================================= OLD =====================================
#OLD FUNCTION TO RENAME THE SCREENSHOTS FILES --> not useful anymore

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