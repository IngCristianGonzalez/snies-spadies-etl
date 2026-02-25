import argparse
from config.database import engine
from etl.extract import extract_excel
from etl.transform import clean_snies
from etl.control import check_year_loaded
from etl.transform import filter_instituciones
from etl.validate.validate import validate_inscritos
from etl.transform import clean_inscritos, filter_instituciones
from etl.loads.load_facts import load_fact_inscritos
from etl.control import register_year
from etl.loads.load_dim_tiempo import load_dim_tiempo
from etl.loads.load_dim_sexo import load_dim_sexos
# def run_snies(path, anio):

#     if check_year_loaded(engine, "fact_inscritos", anio):
#         print(f"El año {anio} ya fue cargado.")
#         return

#     df = extract_excel(path)
#     df = clean_inscritos(df)
#     df = filter_instituciones(df, codigos=["1120", "1123"])
#     validate_inscritos(df)
#     print("Transformación completada.")
#     print(f"Filas procesadas: {len(df)}")



# def run_inscritos(path, anio):

#     df = extract_excel(path)
#     df = clean_inscritos(df)
#     df = filter_instituciones(df, ["1120", "1123"])

#     validate_inscritos(df)

#     load_fact_inscritos(engine, df)

#     register_year(engine, "fact_inscritos", anio)

#     print(f"Año {anio} cargado correctamente.")

#Carga de dimension tiempo
def load_dim_tiempos(path):
    df = extract_excel(path)
    load_dim_tiempo(engine,df)


#Carga de dimension sexo
def load_dim_sexo(path):
    df = extract_excel(path)
    load_dim_sexos(engine,df)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    #parser.add_argument("--year", required=True, type=int)

    args = parser.parse_args()

    # run_snies(args.file, args.year)
    #run_inscritos(args.file, args.year)
    
    #load_dim_tiempos(args.file)
    load_dim_sexo(args.file)


