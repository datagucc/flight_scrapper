# ALL EXTRACTION FUNCTIONS
# Logic for data extraction using GET requests

# MODULE IMPORTS
import requests
import pandas as pd
from datetime import datetime, timedelta
from deltalake import write_deltalake, DeltaTable
from deltalake.table import TableOptimizer
from deltalake.exceptions import TableNotFoundError
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from Modules.metadata_functions import *
import pyarrow as pa





def get_data(base_url, endpoint, data_field=None, params=None, headers=None):
    """
    Performs a GET request to an API to retrieve data.

    Args:
        base_url (str): The base URL of the API (e.g., https://api.luchtmeetnet.nl/open_api).
        endpoint (str): The API endpoint to query (e.g., "components", "stations").
        data_field (str, optional): The JSON field containing the data (e.g., "data").
        params (dict, optional): Query parameters for the request.
        headers (dict, optional): Headers for the request.

    Returns:
        tuple: A tuple containing the data (dict) and the count (int) if available, or None if an error occurs.
    """
    try:
        #estamos concatendo el url y el endpoint 
        endpoint_url = f"{base_url}/{endpoint}"

        #pasamos la nueva url, y los params y headers 
        response = requests.get(endpoint_url, params=params, headers=headers)
        status_request = response.status_code


        #si hay un error, no pasa nada, va a raise una excepcion 
        response.raise_for_status()  # Levanta una excepción si hay un error en la respuesta HTTP.

        # Verificar si los datos están en formato JSON.
        try:
            data = response.json()
            #verificando si hay un "count" en los resultados
            if "count" in data:
                count_nb = data["count"]
            else:
                count_nb = None
            #if el data_field existe, accedo al campo "data_field" en el json y lo paso al variable data
            if data_field:
              if data_field in data:
                  data = data[data_field]
              else:
                  print('there is no such data field')
            
        except:
            print("The format of the response is not the one expected")
            return None
        
        return data , count_nb

    except requests.exceptions.RequestException as e:
        # Capturar cualquier error de solicitud, como errores HTTP.
        print(f"The request has failed. Error code : {e}")
        return None



def build_table(json_data):
    """
    Converts JSON data into a pandas DataFrame.

    Args:
        json_data (dict): JSON data retrieved from an API.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the structured data, or None if an error occurs.
    """
    try:
        return pd.json_normalize(json_data)
    except:
        print("The data format is not as expected.")
        return None


def save_data_as_delta(df, path, mode="overwrite", partition_cols=None, layer='bronze',source=None, author=None):
    """
    Saves a DataFrame in Delta Lake format.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        path (str): The destination path for the Delta Lake table.
        mode (str): Save mode ("overwrite", "append", etc.).
        partition_cols (list or str, optional): Columns to partition the data by.
    """
    write_deltalake(path, df, mode=mode, partition_by=partition_cols)

      #on récupère la deltatable que l'on vient de créer
    my_dt = DeltaTable(path).to_pandas()
        #sauvergarder automatiquement les metadata
    log_metadata(my_dt, table_path = path, layer = layer, source = source, author = author)


def save_new_data_as_delta(new_data, data_path, predicate, partition_cols=None, layer='bronze',source=None, author=None):
    """
    Saves only new data to a Delta Lake table using a MERGE operation.

    Args:
        new_data (pd.DataFrame): The new data to save.
        data_path (str): The Delta Lake table path.
        predicate (str): The condition for the MERGE operation.
        partition_cols (list or str, optional): Columns to partition the data by.
    """
    try:
        dt = DeltaTable(data_path)
        new_data_pa = pa.Table.from_pandas(new_data)
        dt.merge(
            source=new_data_pa,
            source_alias="source",
            target_alias="target",
            predicate=predicate
        ).when_not_matched_insert_all().execute()
        
    except TableNotFoundError:
        save_data_as_delta(new_data, data_path, partition_cols=partition_cols)

    #on récupère la deltatable que l'on vient de créer
    my_dt = DeltaTable(data_path).to_pandas()
    #sauvergarder automatiquement les metadata
    log_metadata(df=my_dt, table_path=data_path, layer = layer, source=source, author=author)


def upsert_data_as_delta(data, data_path, predicate, partition_cols=None ,layer='bronze',source=None, author=None):
    """
    Performs an upsert operation on a Delta Lake table.

    Args:
        data (pd.DataFrame): The data to upsert.
        data_path (str): The Delta Lake table path.
        predicate (str): The condition for the MERGE operation.
    """
    try:
        dt = DeltaTable(data_path)
        data_pa = pa.Table.from_pandas(data)
        dt.merge(
            source=data_pa,
            source_alias="source",
            target_alias="target",
            predicate=predicate
        ).when_matched_update_all().when_not_matched_insert_all().execute()
    except TableNotFoundError:
        save_data_as_delta(data, data_path,partition_cols=partition_cols)

    #sauvergarder automatiquement les metadata
    my_dt = DeltaTable(data_path).to_pandas()
    log_metadata(df=my_dt, table_path=data_path, layer = layer, source=source, author=author)


def read_most_recent_partition(data_path):
    """
    Reads the most recent partition from a Delta Lake table.

    Args:
        data_path (str): The Delta Lake table path.

    Returns:
        pd.DataFrame: Data from the most recent partition, or None if an error occurs.
    """
    try:
        requested_date = datetime.utcnow() - timedelta(hours=1)
        dt = DeltaTable(data_path)
        return dt.to_pandas(partitions=[
            ("fecha", "=", requested_date.strftime("%Y-%m-%d")),
            ("hora", "=", requested_date.strftime("%H"))
        ])
    except Exception as e:
        print(f"Failed to process the Delta Lake table. Error: {e}")
        return None


def load_platform_dataset(path_file, platform_name):
    """
    Loads a CSV file containing streaming platform data.

    Args:
        path_file (str): Path to the CSV file.
        platform_name (str): Name of the streaming platform.

    Returns:
        pd.DataFrame: A DataFrame with the data and an additional "platform_name" column, or None if an error occurs.
    """
    try:
        df_platform = pd.read_csv(path_file)
        df_platform["platform_name"] = platform_name
        return df_platform
    except FileNotFoundError:
        print(f"Error: File '{path_file}' not found.")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None


def fill_null_values(df, column_name, fill_value):
    """
    Fills null values in a specified column with a given value.

    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        column_name (str): The column to fill null values in.
        fill_value: The value to fill nulls with.

    Returns:
        pd.DataFrame: The updated DataFrame.
    """
    df[column_name] = df[column_name].fillna(fill_value)
    return df


def explode_column(df_origin, cols_to_select, col_to_explode):
    """
    Explodes a column with comma-separated values into separate rows.

    Args:
        df_origin (pd.DataFrame): The original DataFrame.
        cols_to_select (list): Columns to retain from the original DataFrame.
        col_to_explode (str): The column to explode.

    Returns:
        pd.DataFrame: A new DataFrame with the exploded column, or None if an error occurs.
    """
    try:
        df_result = df_origin[cols_to_select].copy()
        df_result[col_to_explode] = df_result[col_to_explode].str.split(", ")
        return df_result.explode(col_to_explode)
    except KeyError as e:
        print(f"Columns not found in the DataFrame: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    

def merging_df(df1,df2,type_merge,df1_keys, df2_keys, df2_columns):
    """
    Merging two dataframes with a specified merge type and keys.
    
    Parameters:
    df1 can be either a str or a dataframe
                        df1 : pd.DataFrame : The first DataFrame to merge.
                        df1 : str : The path to the first DataFrame.
    df2 can be either a str or a dataframe
                        df2 : pd.DataFrame : The second DataFrame to merge.
                        df2 : str : The path to the second DataFrame.

    type_merge (str): The type of merge to perform ('left', 'right', 'inner', 'outer').
    df1_keys (list): The keys from the first DataFrame to merge on.
    df2_keys (list): The keys from the second DataFrame to merge on.
    df2_columns (list): The columns from the second DataFrame to include in the merged result.
    
    Returns:
    pd.DataFrame: The merged DataFrame.
    """
    # Charger df1 si c'est un chemin
    if isinstance(df1, pd.DataFrame):
        df1 = df1
    elif isinstance(df1, str):
        df1 = DeltaTable(df1).to_pandas()
    else:
        raise TypeError("df1 must be a DataFrame or a string path to a DeltaTable.")

    # Charger df2 si c'est un chemin
    if isinstance(df2, pd.DataFrame):
        df2 = df2
    elif isinstance(df2, str):
        df2 = DeltaTable(df2).to_pandas()
    else:
        raise TypeError("df2 must be a DataFrame or a string path to a DeltaTable.")

    # Correct is not list:
    """if not df1_keys.isinstance(list):
        df1_keys = list(df1_keys)
    if not df2_keys.isinstance(list):
        df2_keys = list(df2_keys)
    if not df2_columns.isinstance(list):    
        df2_columns = list(df2_columns)
    """
    #Perform the merge
    dt_merge = pd.merge(
        df1,
        df2[df2_columns],  
        how=type_merge,
        left_on=df1_keys,
        right_on=df2_keys
    )
    
    return dt_merge



def aggregate_dataframe(df, groupby_cols, agg_dict, count_col = None, count_col_name = 'n_rows_aggregated'):
    """
    Agrège un DataFrame avec possibilité de plusieurs fonctions par colonne,
    et renomme automatiquement les colonnes agrégées.

    Paramètres :
    -----------
    df : pd.DataFrame
        DataFrame source
    groupby_cols : list
        Colonnes pour le groupby
    agg_dict : dict
        Dictionnaire {colonne: fonction(s)}. Valeurs possibles :
            - str (ex: 'mean')
            - fonction (ex: lambda x: ...)
            - liste de str/fonctions (ex: ['mean', 'max'])

    Retour :
    --------
    pd.DataFrame
        DataFrame agrégé avec colonnes renommées
    """



    # Appliquer le groupby + aggregation
    grouped = df.groupby(groupby_cols).agg(agg_dict)

    # WE COULD IMPROVE THE FUNCTION BY ADDING THE COUNT OF THE GROUPED COLUMNS
   # if count_col:
    #    count_series = df.groupby(groupby_cols)[count_col].count().rename(count_col_name)
     #   grouped = grouped.join(count_series)
    

    # Renommer les colonnes
    if isinstance(grouped.columns, pd.MultiIndex):
        # Si plusieurs agg par colonne -> MultiIndex : ('col', 'agg')
        grouped.columns = [
            f"{col}_{func.__name__.upper() if callable(func) else func.upper()}"
            for col, func in grouped.columns
        ]
    else:
        # Une seule agg par colonne
        grouped.columns = [
            f"{col}_{func.__name__.upper() if callable(func) else func.upper()}"
            for col, func in agg_dict.items()
        ]

    return grouped.reset_index()



def order_table(source, sort_columns):
    """
    Trie une table Delta ou un DataFrame selon une ou plusieurs colonnes.

    Args:
        source (str | pd.DataFrame): Chemin vers une Delta Table OU un DataFrame en mémoire.
        sort_columns (str | list[str]): Colonne ou liste de colonnes à utiliser pour le tri.

    Returns:
        pd.DataFrame: Le DataFrame trié.
    """
    # Gestion mono-colonne vs multi-colonnes
    if isinstance(sort_columns, str):
        sort_columns = [sort_columns]

    # Charger la table si c'est un chemin
    if isinstance(source, str):
        df = DeltaTable(source).to_pandas()
    else:
        df = source.copy()

    # Tri
    return df.sort_values(by=sort_columns).reset_index(drop=True)





def compact_all_silver_tables(meta_data, layer_filter= "Silver"):
    """
    Compacte toutes les Delta Tables du layer Silver listées dans la table meta_data.

    Args:
        meta_data (pd.DataFrame): Table des métadonnées contenant les chemins des tables.
        layer_filter (str): Le layer à filtrer pour ne compacter que les tables Silver.
    """
    silver_tables = meta_data[meta_data["layer"] == layer_filter]

    for path in silver_tables["table_path"]:
        try:
            dt = DeltaTable(path)
            TableOptimizer(dt).compact()
            print(f"✅ Compacté : {path}")
        except Exception as e:
            print(f"❌ Erreur sur {path} : {e}")
