import pandas as pd

def extract_excel(path):
    print(f"Extrayendo archivo: {path}")
    return pd.read_excel(path)

def extract_csv(path):
    print(f"Extrayendo archivo: {path}")
    return pd.read_csv(path, sep=";")
