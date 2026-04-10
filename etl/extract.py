import pandas as pd

# Funcion que extrae datos de un archivo Excel, soportando tanto .xlsx como .xlsb
import pandas as pd

def extract_excel(path):

    if path.endswith(".xlsx"):
        temp = pd.read_excel(path, engine="openpyxl", header=None)
        engine_used = "openpyxl"

    elif path.endswith(".xlsb"):
        temp = pd.read_excel(path, engine="pyxlsb", header=None)
        engine_used = "pyxlsb"

    else:
        raise ValueError(f"Formato no soportado: {path}")

    # 🔍 detectar header usando "semestre"
    header_row = None

    for i, row in temp.iterrows():
        row_str = " ".join([str(x).lower() for x in row.values])

        if "semestre" in row_str:
            header_row = i
            break

    # 🔥 fallback por si acaso
    if header_row is None:
        print("⚠️ No se detectó header, usando fila 2")
        header_row = 2

    # 🔹 leer archivo correctamente
    df = pd.read_excel(path, engine=engine_used, header=header_row)

    return df  


def extract_csv(path):
    print(f"Extrayendo archivo: {path}")
    return pd.read_csv(path, sep=";")
