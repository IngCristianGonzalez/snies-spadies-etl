import os
from config.database import engine
from etl.extract import extract_excel
import pandas as pd
from etl.loads.load_dim_departamentos import load_dim_departamentos
from etl.loads.load_dim_municipios import load_dim_municipios
from etl.loads.load_dim_institucion import load_dim_institucion
from etl.loads.load_dim_sexo import load_dim_sexos
from etl.loads.load_dim_tiempo import load_dim_tiempo
from etl.loads.load_dim_programa import load_dim_programas
from etl.loads.load_dim_programa_oferta import load_dim_programa_oferta


def procesar_archivo(path):

    nombre = os.path.basename(path).lower()

    print(f"\n📂 Procesando dimensión: {nombre}")

    import pandas as pd
    df = pd.read_excel(path)   # 🔥 CAMBIO IMPORTANTE

    print("🧾 Columnas:", df.columns.tolist())

    if "departamento" in nombre:
        load_dim_departamentos(engine, df)

    elif "municipio" in nombre:
        load_dim_municipios(engine, df)

    elif "institucion" in nombre:
        load_dim_institucion(engine, df)

    elif "sexo" in nombre:
        load_dim_sexos(engine, df)

    elif "tiempo" in nombre:
        load_dim_tiempo(engine, df)

    elif "programa" in nombre:
        load_dim_programas(engine, df)
    elif "programa_oferta" in nombre:
        load_dim_programa_oferta(engine, df)


def procesar_carpeta(ruta):

    for archivo in os.listdir(ruta):

        # ignorar temporales de Excel
        if archivo.startswith("~$"):
            continue

        if archivo.endswith((".xlsx", ".xlsb")):
            path = os.path.join(ruta, archivo)
            procesar_archivo(path)


if __name__ == "__main__":

    # ruta = "data/dimensions"   # 🔥 ajusta si tu carpeta es distinta
    # procesar_carpeta(ruta)

    ruta = "data/dimensions"

    archivos = {
        "departamento": "dim_departamentos.xlsx",
        "municipio": "dim_municipio.xlsx",
        "institucion": "dim_instituciones.xlsx",
        "sexo": "dim_sexo.xlsx",
        "tiempo": "dim_tiempo.xlsx",
        "programa": "programas.xlsx"
    }

    for key in ["departamento", "municipio", "institucion", "sexo", "tiempo","programa"]:
        path = os.path.join(ruta, archivos[key])
        procesar_archivo(path)