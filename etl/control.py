from sqlalchemy import text
import pandas as pd

def check_year_loaded(engine, tabla, anio, tipo):

    query = text("""
        SELECT 1
        FROM etl_control
        WHERE tabla = :tabla
        AND anio = :anio
        AND tipo = :tipo
        LIMIT 1
    """)

    result = pd.read_sql(
        query,
        engine,
        params={
            "tabla": tabla,
            "anio": anio,
            "tipo": tipo
        }
    )

    return not result.empty

def register_year(engine, tabla, anio, tipo):

    query = text("""
        INSERT INTO etl_control (tabla, anio, tipo)
        VALUES (:tabla, :anio, :tipo)
    """)

    with engine.begin() as conn:
        conn.execute(query, {
            "tabla": tabla,
            "anio": anio,
            "tipo": tipo
        })