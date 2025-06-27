from datetime import date
import sys
import os
import time
#test OCR
import pytesseract
import cv2
from DF_functions import *
import pandas as pd
from dateutil.relativedelta import relativedelta


def rename_files_by_month(path, start_year, start_month):
    """
    Renomme les fichiers d'un dossier en les associant à des mois successifs,
    en commençant par le mois et l'année donnés.
    Crée une table meta_data pour stocker les informations sur les fichiers traités.
    
    Args:
        path (str): chemin vers le dossier contenant les fichiers.
        start_year (int): année de départ (ex: 2025).
        start_month (int): mois de départ (1 à 12).
    """
    # Vérifier que le dossier existe
    if not os.path.isdir(path):
        print(f"❌ Le dossier {path} n'existe pas.")
        return

    # Extraire le nom du dossier pour le nommage des fichiers
    name_folder = os.path.basename(os.path.normpath(path))

    # Lister les fichiers et les trier par date de création (plus ancien en premier)
    files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    files = sorted(files, key=lambda f: os.path.getctime(os.path.join(path, f)))
    date_of_search = files[0][17:27]

    # Date de départ
    date = datetime(start_year, start_month, 1)

    # Renommer les fichiers
    for i, filename in enumerate(files):
        file_ext = os.path.splitext(filename)[1]  # Garde l'extension (.png, .jpg, etc.)
        new_date = date + relativedelta(months=i)
        month_name = new_date.strftime("%B")      # Nom du mois en anglais
        year = new_date.year
        new_name = f"{name_folder}_{month_name}_{year}_on_{date_of_search}{file_ext}"
        
        old_path = os.path.join(path, filename)
        new_path = os.path.join(path, new_name)

        os.rename(old_path, new_path)
        print(f"✅ {filename} renommé en {new_name}")


def call_ocr(folder_name, image_name, save_as=False):
    """
    Fonction pour appeler l'OCR sur une image donnée.
    De plus, cette fonctionne enregistre l'image en niveaux de gris et seuillée.
    Args:
        folder_name (str): Nom du dossier contenant l'image.
        image_name (str): Nom de l'image à traiter.
    Returns:
        str: Texte extrait de l'image.
    
    """
    my_folder = folder_name
    #my_folder = '/Users/focus_profond/GIT_repo/IDLE_CITY_PROJECT/Accessibility/Data_Sources/screenshot_skyscanner'
    os.chdir(my_folder)
    # 1. Charger l’image
    image = cv2.imread(image_name)

    # 2. Convertir en niveaux de gris
    if image is None:
        print(f"Erreur : l'image {image_name} n'a pas pu être chargée.")
        return ""
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    #2.3 on enregistre l'image en niveaux de gris
    name_grey = image_name+'_gray.png'

    #cv2.imwrite("ocr_sample_gray_5.png", gray)

    #2.5 Seuillage de l'image
    _, thresh = cv2.threshold(gray, 150, 255, cv2.THRESH_BINARY)
    #2.6 on enregistre l'image seuillée
    if save_as:
        name_tresh = image_name+'_tresh.png'
        cv2.imwrite(name_tresh, thresh)


    # 3. OCR avec Tesseract
    text = pytesseract.image_to_string(gray)

    # 4. Return le résultat
    return(text)




def parse_text_to_raw_dict(text: str) -> dict:
    """
    Transforme un texte OCR en dictionnaire brut indexé par lignes,
    où chaque ligne est une liste de tokens (mots/chiffres/symboles).
    """
    raw_dict={}
    count = 0
    for i, line in enumerate(text.split('\n')):
    #if ("1" or "2" or "3" or "4" or "5" or "6" or "7" or "8" or "9" or "€")  in line:
       # print(f"Line {i}: {line}")
        #print(f"Line {count} : {line}")

        if line =="":
                continue
        line = line.split()
        raw_dict[count] = line
        count += 1
    return raw_dict



def merge_orphan_euros(row):
    """
    Fusionne les symboles '€' orphelins avec le chiffre précédent.
    Exemple : ['432', '€'] → ['432€']
    """
    new_row = []
    for item in row:
        if item == '€' and new_row:
            # Colle le symbole euro au dernier élément si c'est un chiffre
            new_row[-1] += '€'
        else:
            new_row.append(item)
    return new_row

def clean_all_rows(raw_dict: dict) -> dict:
    """
    Applique le nettoyage (merge des '€') à toutes les lignes d'un dictionnaire.
    """
    # Application sur toutes les lignes du dictionnaire
    cleaned_data = {k: merge_orphan_euros(v) for k, v in raw_dict.items()}
    return cleaned_data


def extract_day_price_pairs(cleaned_dict,name_file, trip= "empty"):
    """
    Extrait un dictionnaire {jour: prix} à partir des lignes basées sur leur longueur.
    On suppose que les lignes avec même longueur sont des paires date/prix.
    """
    result = {}
    keys = sorted(cleaned_dict.keys(), reverse=True)
    i = 0
    date_of_search = name_file[8:18]
    month_date = name_file[-5]
    if month_date in (1,2,3,4):
        month_date = month_date+'-2026'
    else:
        month_date = str(month_date)+'-2025'
    while i < len(keys) - 1:
        row_price = cleaned_dict[keys[i]]
        row_date = cleaned_dict[keys[i + 1]]
        if len(row_price) == len(row_date):
            for day, price in zip(row_date, row_price):
                day = str(day)+'-' + month_date  # Ajout du mois
                result[day] = price
            i += 2  # on saute deux lignes : jour + prix
        else:
            i += 1  # si tailles différentes, on avance juste
    sorted_dict = dict(sorted(result.items()))
    my_df = pd.json_normalize(sorted_dict)
    my_df =my_df.transpose()
    my_df['trip'] = trip
    my_df['name_file'] = name_file
    my_df['date_of_search'] = date_of_search
    my_df = my_df.reset_index()
    my_df.columns = [ 'flight_date','price', 'trip','name_file','date_of_search']
    return my_df

#améliorer cette fonction pour qu'elle soit plus dynamique 
#def storing_data(df, directory='/Users/focus_profond/GIT_repo/IDLE_CITY_PROJECT/Accessibility', source = 'screenshots', author = 'Augustin'):
def storing_data(df, source = 'screenshots', author = 'Augustin'):
    
    main_directory = '/Users/focus_profond/GIT_repo/IDLE_CITY_PROJECT/Accessibility'
    os.chdir(main_directory)
    name_folder = main_directory+ '/Data/Bronze/BigTable'
    partition_cols = None
    predicate = "target.flight_date = source.flight_date AND target.trip = source.trip AND target.date_of_search = source.date_of_search"

    save_new_data_as_delta(df,name_folder,predicate= predicate, partition_cols=partition_cols, layer = 'Bronze', source= source, author =author)


def full_function(trip, source_dir):
    my_folder = source_dir + trip
    names = os.listdir(my_folder)
    print(my_folder)
    for name in names:
        print(f"We start to store the image : {name} from the folder : {name}")
        text = call_ocr(my_folder, name)
        raw_dict = parse_text_to_raw_dict(text)
        cleaned_dict = clean_all_rows(raw_dict)
        day_price_df = extract_day_price_pairs(cleaned_dict,name,trip)
        storing_data(day_price_df)
        print("The image :", name, "has been processed and stored in the database.")
    


