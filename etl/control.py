from sqlalchemy import text

def check_year_loaded(engine, tabla, anio):
    query = text("""
        SELECT 1
        FROM etl_control
        WHERE tabla = :tabla
        AND anio = :anio
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"tabla": tabla, "anio": anio}).fetchone()
        return result is not None

def register_year(engine, tabla, anio):
    query = text("""
        INSERT INTO etl_control (tabla, anio)
        VALUES (:tabla, :anio)
    """)
    with engine.begin() as conn:
        conn.execute(query, {"tabla": tabla, "anio": anio})
