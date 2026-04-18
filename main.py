import os

from numpy import rint
from config.database import engine
from etl.extract import extract_excel
from etl.loads.load_dim_programa_oferta import load_dim_programa_oferta
from etl.transform import transformar_snies
from etl.validate.validate import validate_snies_generic
from etl.loads.load_facts import load_fact_snies
from etl.control import check_year_loaded, register_year


def detectar_tipo_archivo(nombre):
    nombre = nombre.lower()
    if "inscritos" in nombre:
        return "inscritos"
    elif "admitidos" in nombre:
        return "admitidos"
    elif "matriculados" in nombre:
        return "matriculados"
    elif "graduados" in nombre:
        return "graduados"
    return "otro"


def procesar_archivo(path):

    nombre = os.path.basename(path)
    anio = int(path.split(os.sep)[-2])

    # ✅ detectar tipo correctamente
    tipo = detectar_tipo_archivo(nombre)

    print(f"\n📂 Procesando {nombre} - Año {anio} - Tipo {tipo}")

    if tipo == "otro":
        print(f"⚠️ {nombre} no es un tipo reconocido")
        return

    # ✅ control ETL
    if check_year_loaded(engine, "tb_fact_snies", anio, tipo):
        print(f"⚠️ {tipo} {anio} ya cargado")
        return

    # ✅ EXTRAER
    df = extract_excel(path)

    try:
        df = transformar_snies(df, tipo)
    except Exception as e:
        print(f"❌ Error procesando {nombre}: {e}")
        return  # o continue si estás en loop

    # ✅ DIMENSIONES
    load_dim_programa_oferta(engine, df)

    # ✅ VALIDACIÓN
    validate_snies_generic(df)

    # ✅ FACT
    load_fact_snies(engine, df, tipo)

    # ✅ CONTROL
    register_year(engine, "tb_fact_snies", anio, tipo)

    print(f"✅ {tipo} {anio} cargado correctamente")


def procesar_carpeta(ruta):

    for root, dirs, files in os.walk(ruta):

        print(f"\n📂 Carpeta: {root}")

        for archivo in files:
            print("→ Archivo crudo:", repr(archivo))  # 🔥 clave

            archivo_clean = archivo.strip().lower()

            # 🔥 filtro robusto
            if not archivo_clean.endswith((".xlsx", ".xls", ".xlsb")):
                print(f"⏭️ Ignorado (extensión): {archivo}")
                continue

            path = os.path.join(root, archivo)

            print(f"✅ Procesando: {archivo_clean}")

            try:
                procesar_archivo(path)
            except Exception as e:
                print(f"❌ Error en {archivo}: {e}")
                continue


if __name__ == "__main__":
    ruta = "data/snies"
    procesar_carpeta(ruta)