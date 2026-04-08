import pandas as pd

# Funcion que extrae datos de un archivo Excel, soportando tanto .xlsx como .xlsb
def extract_excel(path):
    if path.endswith(".xlsx"):
        df = pd.read_excel(path, engine="openpyxl")
    elif path.endswith(".xlsb"):
        df = pd.read_excel(path, engine="pyxlsb")
    else:
        raise ValueError(f"Formato no soportado: {path}")
    return df

def extract_csv(path):
    print(f"Extrayendo archivo: {path}")
    return pd.read_csv(path, sep=";")
