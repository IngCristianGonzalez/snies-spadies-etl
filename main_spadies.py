import os
import pandas as pd
from config.database import engine
from etl.transform_spadies import transformar_spadies
from etl.loads.load_spadies import load_spadies


def procesar_csv(path):

    nombre = os.path.basename(path)

    print(f"\n📂 Procesando {nombre}")

    df = pd.read_csv(path, encoding="utf-8-sig", sep=";")

    df = transformar_spadies(df, nombre)

    print(df.head())

    load_spadies(engine, df)

    print(f"✅ {nombre} cargado")


def procesar_carpeta(ruta):

    for archivo in os.listdir(ruta):
        if archivo.endswith(".csv"):
            path = os.path.join(ruta, archivo)
            procesar_csv(path)


if __name__ == "__main__":
    ruta = "data/spadies"
    procesar_carpeta(ruta)