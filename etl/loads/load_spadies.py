from sqlalchemy import text

def load_spadies(engine, df):

    with engine.begin() as conn:

        for _, row in df.iterrows():

            # 🔹 variable
            result = conn.execute(
                text("""
                    INSERT INTO tb_dim_variable_spadies (nombre)
                    VALUES (:nombre)
                    ON CONFLICT (nombre) DO UPDATE SET nombre=EXCLUDED.nombre
                    RETURNING id
                """),
                {"nombre": row["variable"]}
            )
            variable_id = result.fetchone()[0]

            # 🔹 categoria
            result = conn.execute(
                text("""
                    INSERT INTO tb_dim_categoria_spadies (variable_id, valor)
                    VALUES (:variable_id, :valor)
                    ON CONFLICT DO NOTHING
                    RETURNING id
                """),
                {
                    "variable_id": variable_id,
                    "valor": row["categoria"]
                }
            )

            categoria_row = result.fetchone()

            if categoria_row:
                categoria_id = categoria_row[0]
            else:
                categoria_id = conn.execute(
                    text("""
                        SELECT id FROM tb_dim_categoria_spadies
                        WHERE variable_id = :variable_id AND valor = :valor
                    """),
                    {
                        "variable_id": variable_id,
                        "valor": row["categoria"]
                    }
                ).fetchone()[0]

            # 🔹 tiempo
            tiempo = conn.execute(
                text("""
                    SELECT id FROM tb_dim_tiempo
                    WHERE anio = :anio AND semestre = :semestre
                """),
                {
                    "anio": row["anio"],
                    "semestre": row["semestre"]
                }
            ).fetchone()

            if not tiempo:
                continue

            tiempo_id = tiempo[0]

            # 🔹 fact
            conn.execute(
                text("""
                    INSERT INTO tb_fact_spadies (categoria_id, tiempo_id, porcentaje)
                    VALUES (:categoria_id, :tiempo_id, :porcentaje)
                    ON CONFLICT (categoria_id, tiempo_id) DO UPDATE
                    SET porcentaje = EXCLUDED.porcentaje
                """),
                {
                    "categoria_id": categoria_id,
                    "tiempo_id": tiempo_id,
                    "porcentaje": row["porcentaje"]
                }
            )