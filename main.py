import os
from config.database import engine
from etl.extract import extract_excel
from etl.loads.load_dim_programa_oferta import load_dim_programa_oferta
from etl.transform import transformar_snies
from etl.validate.validate import validate_inscritos
from etl.loads.load_facts import load_fact_inscritos
from etl.control import check_year_loaded, register_year


def detectar_tipo_archivo(nombre):
    nombre = nombre.lower()
    if "inscritos" in nombre:
        return "inscritos"
    elif "admitidos" in nombre:
        return "admitidos"
    elif "matriculados" in nombre:
        return "matriculados"
    return "otro"


def procesar_archivo(path):

    nombre = os.path.basename(path)
    anio = int(path.split(os.sep)[-2])
    tipo = detectar_tipo_archivo(nombre)

    print(f"\n📂 Procesando {nombre} - Año {anio} - Tipo {tipo}")

    if check_year_loaded(engine, f"fact_snies_{tipo}", anio):
        print(f"⚠️ {tipo} {anio} ya cargado")
        return

    df = extract_excel(path)
    df = transformar_snies(df)
    load_dim_programa_oferta(engine, df)  

    validate_inscritos(df)

    load_fact_inscritos(engine, df)

    register_year(engine, f"fact_snies_{tipo}", anio)

    print(f"✅ {tipo} {anio} cargado correctamente")


def procesar_carpeta(ruta):

    for root, dirs, files in os.walk(ruta):
        for archivo in files:
            if archivo.endswith((".xlsx", ".xlsb")):
                path = os.path.join(root, archivo)
                procesar_archivo(path)


if __name__ == "__main__":
    ruta = "data/snies"
    procesar_carpeta(ruta)