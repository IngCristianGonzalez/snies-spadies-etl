import argparse
from config.database import engine
from etl.extract import extract_excel
from etl.transform import clean_snies
from etl.control import check_year_loaded
from etl.transform import filter_instituciones

def run_snies(path, anio):

    if check_year_loaded(engine, "fact_inscritos", anio):
        print(f"El año {anio} ya fue cargado.")
        return

    df = extract_excel(path)
    df = clean_snies(df)
    df = filter_instituciones(df, codigos=["1120", "1123"])
    print("Transformación completada.")
    print(f"Filas procesadas: {len(df)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    parser.add_argument("--year", required=True, type=int)

    args = parser.parse_args()

    run_snies(args.file, args.year)


