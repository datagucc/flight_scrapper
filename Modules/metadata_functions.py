"""
There are severals types of metadata :
- technical
- business
- operational
- gouvernance 


Chaque écriture ou update de table devrait mettre à jour automatiquement les métadonnées associées.
"""

# metadata.py

import os
from datetime import datetime, timedelta
import pandas as pd
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError
import pyarrow as pa
import getpass
import platform
import json

def get_author_info():
    """
    Retourne une chaîne d'identification de l'auteur ou système.
    """
    try:
        user = getpass.getuser()
        hostname = platform.node()
        return f"{user}@{hostname}"
    except Exception:
        return "unknown"


def get_delta_table_stats(delta_table_path) :
    """
    Calcule la taille totale et le nombre de fichiers d'une DeltaTable.

    Args:
        delta_table_path (str): Chemin vers la DeltaTable.

    Returns:
        dict: Dictionnaire avec la taille (en Mo) et le nombre de fichiers.
    """
    total_size = 0
    file_count = 0

    #os.walk : parcourt recursivement tous les sours-dossiers du delta table path
    for dirpath, _, filenames in os.walk(delta_table_path):
        for f in filenames:
            #pour chaque fichier, on incrémente le compteur file_count
            file_count += 1
            #on construit son chemin absolu
            fp = os.path.join(dirpath, f)
            #on vérifie que ce chemin correspond bien à un fichier
            if os.path.isfile(fp):
                #on calcule et on ajoute sa taille au total size
                total_size += os.path.getsize(fp)

    return {
        "delta_table_size_MB": round(total_size / (1024 * 1024), 2),
        "delta_file_count": file_count
    }


def inspect_dataframe(df,delta_table_path = None):
    """
    Analyse un DataFrame et retourne des statistiques simples :
    - Nombre total de lignes
    - Nombre de lignes contenant au moins une valeur nulle
    - Nombre de lignes dupliquées (sur toutes les colonnes)
    - [optionnel] Taille et nombre de fichiers si chemin vers DeltaTable fourni

    Args:
        df (pd.DataFrame): Le DataFrame à analyser.
        delta_table_path (str, optional): Chemin vers la DeltaTable liée.


    Returns:
        dict: Dictionnaire contenant les statistiques.
    """
    stats ={ 
        "total_rows" : len(df)
            ,"rows_with_nulls" : df.isnull().any(axis=1).sum()
            ,"duplicate_rows" : df.duplicated().sum()
              #    "total_rows" : 1
           #,"rows_with_nulls" :0
           #,"duplicate_rows" : 0
          }

    if delta_table_path:
        stats.update(get_delta_table_stats(delta_table_path))
    else:
        stats['delta_table_size_MB']=None
        stats['delta_file_count']=None
    #print(stats['duplicate_rows'])
    #print(df.duplicated().sum())

    return stats


def log_metadata(df, table_path, layer, source=None, author=None, metadata_store_path="Data/_meta/metadata_table"):
    """
    Enregistre ou met à jour les métadonnées pour une table Delta donnée.

    Args:
        df (pd.DataFrame): Le DataFrame stocké.
        table_path (str): Le chemin de la table Delta.
        layer (str): Nom de la couche ("bronze", "silver", "gold").
        source (str, optional): La source d'origine (API, CSV, etc.).
        author (str, optional): Le nom de l'auteur ou du système ayant généré les données.
        metadata_store_path (str): Chemin vers la table de métadonnées.
    """

    try:
        try:
        # normalement la table aura déjà été créée dans Data, donc il est déjà possible de calculer sa taille et le nbr de fichier.
        # on tente de calculer la taille et le nbr de fichier
        #pour cela, il nous faut juste le path de la deltatable, ce qu'on a : table_path
        #size_count = get_delta_table_stats(table_path)
            stats = inspect_dataframe(df, table_path)
            #print('stats : ',stats)
            total_rows = stats['total_rows']
            rows_with_nulls = stats['rows_with_nulls']
            duplicate_rows = stats['duplicate_rows']
            delta_table_size_MB = stats['delta_table_size_MB']
            delta_file_count = stats['delta_file_count']
        except :
            total_rows = 'None'
            rows_with_nulls = 'None'
            duplicate_rows = 'None'
            delta_table_size_MB='None'
            delta_file_count='None'
        
        # Valeurs de base
        updated_at = datetime.utcnow()
        created_at = updated_at  # Valeur par défaut (sera mise à jour si la table existe déjà)

        # Si la metadata table existe, on regarde s’il y a déjà une entrée
        if os.path.exists(metadata_store_path):
            try:
                dt = DeltaTable(metadata_store_path)
                existing_df = dt.to_pandas()
                existing_entry = existing_df[existing_df["table_path"] == table_path]

                if not existing_entry.empty:
                    # Si l'entrée existe déjà → on garde l'ancien created_at
                    created_at = pd.to_datetime(existing_entry["created_at"].iloc[0])
            except:
                pass  # en cas d'échec, on garde la valeur par défaut


        metadata = {
            "table_path": table_path,
            "table_name": os.path.basename(table_path),
            "layer": layer,
            "total_rows": total_rows,
            "rows_with_nulls": rows_with_nulls,
            "rows_duplicated": duplicate_rows,
            "columns": json.dumps(df.columns.tolist()),
            "dtypes": json.dumps({col: str(dtype) for col, dtype in df.dtypes.items()}),
            "delta_table_size_MB":delta_table_size_MB,
            "file_count":delta_file_count,
            #"columns": df.columns.tolist(),
            #"dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "updated_at": updated_at,
            "created_at": created_at,
            "source": source or "unknown",
            "author": author or get_author_info()
            
        }

        metadata_df = pa.Table.from_pandas(pd.DataFrame([metadata]))
        # Mise à jour ou création de la table de métadonnées
        if os.path.exists(metadata_store_path):
            try:
                dt = DeltaTable(metadata_store_path)
                
                dt.merge(
                    source=metadata_df,
                    source_alias="source",
                    target_alias="target",
                    predicate="source.table_path = target.table_path"
                ).when_matched_update_all().when_not_matched_insert_all().execute()
            except TableNotFoundError:
                
                write_deltalake(metadata_store_path, metadata_df, mode="overwrite")

        else:
            write_deltalake(metadata_store_path, metadata_df, mode="overwrite")
        



    except Exception as e:
        print(f"[Metadata] Échec de l'enregistrement des métadonnées : {e}")


def export_metadata_to_excel(layer= "all"):
    """
    Exporte la table Delta de métadonnées en fichier Excel filtré par layer.
    
    Paramètres :
    ------------
    layer : str
        Le niveau de la couche à exporter : 'bronze', 'silver', 'gold' ou 'all'
    """
    
    # Définir le chemin vers la DeltaTable
    name_folder = 'Data/_meta/metadata_table'
    timestamp = datetime.now().strftime("%Y-%m-%d")
    timestamp = str(datetime.now().strftime("%Y-%m-%d"))
    export_folder = 'logs/'+timestamp
    os.makedirs(export_folder, exist_ok=True)

    # Charger la table Delta et convertir en DataFrame
    dt = DeltaTable(name_folder).to_pandas()

    # Filtrage selon le layer
    layer = layer.lower()
    if layer in ['bronze', 'silver', 'gold']:
        dt = dt[dt['layer'].str.lower() == layer]
    elif layer != 'all':
        raise ValueError("Layer invalide. Choisir parmi 'Bronze', 'Silver', 'Gold', ou 'all'.")

    # Générer un nom de fichier avec timestamp
    timestamp = datetime.now().strftime("%H")
    name= str(datetime.now().strftime("%H"))+'h'+str(datetime.now().strftime("%M"))
    filename = f"{layer}_metadata_{name}.xlsx"
    filepath = os.path.join(export_folder, filename)

    # Exporter le DataFrame en fichier Excel
    dt.to_excel(filepath, index=False)

    print(f"✅ Métadonnées exportées avec succès dans : {filepath}")
