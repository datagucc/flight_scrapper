import pandas as pd
import numpy as np

# Mapping des types logiques -> Pandas
PANDAS_TYPE_MAP = {
    "string": "string",
    "int": "Int64",         # Permet les int nullables
    "float": "Float64",     # Idem pour les float
    "bool": "boolean",      # Booléen nullable
    "datetime": "datetime64[ns]",
    "object": "object"      # Fallback
}

def cast_dataframe_types(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """
    Caste les colonnes d'un DataFrame en fonction du schéma donné.

    Args:
        df (pd.DataFrame): Le DataFrame à caster.
        schema (dict): Dictionnaire {colonne: type_logique}

    Returns:
        pd.DataFrame: Le DataFrame casté.
    """
    df = df.copy()
    for col, typ in schema.items():
        try:
            if typ not in PANDAS_TYPE_MAP:
                raise ValueError(f"Type inconnu: {typ} pour la colonne {col}")
            pandas_type = PANDAS_TYPE_MAP[typ]

            # Cas datetime explicite
            if typ == "datetime":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(pandas_type)

        except Exception as e:
            print(f"[cast_types] Erreur lors du cast de '{col}' en {typ} : {e}")
            df[col] = pd.NA  # Fallback sécurisant

    return df
