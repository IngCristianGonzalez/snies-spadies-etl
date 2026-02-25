import pandas as pd

def load_fact_inscritos(engine, df):

    print("Preparando carga de tb_fact_inscritos...")

    # ---- Cargar dimensiones ----
    dim_programa = pd.read_sql(
        "SELECT id, codigo_snies FROM tb_dim_programa",
        engine
    )

    dim_institucion = pd.read_sql(
        "SELECT id, codigo_ies FROM tb_dim_institucion",
        engine
    )

    dim_municipio = pd.read_sql(
        "SELECT id, codigo_municipio FROM tb_dim_municipio",
        engine
    )

    dim_ubicacion = pd.read_sql(
        "SELECT id, municipio_id FROM tb_dim_ubicacion_oferta",
        engine
    )

    dim_programa_oferta = pd.read_sql("""
        SELECT id, programa_id, institucion_id, ubicacion_oferta_id
        FROM tb_dim_programa_oferta
    """, engine)

    dim_sexo = pd.read_sql(
        "SELECT id, descripcion FROM tb_dim_sexo",
        engine
    )

    dim_tiempo = pd.read_sql(
        "SELECT id, anio, semestre FROM tb_dim_tiempo",
        engine
    )

    # ---- MERGES ----

    df = df.merge(dim_programa,
                  left_on="codigo_snies_del_programa",
                  right_on="codigo_snies")

    df = df.merge(dim_institucion,
                  left_on="codigo_de_la_institucion",
                  right_on="codigo_ies")

    df = df.merge(dim_municipio,
                  left_on="codigo_del_municipio_programa",
                  right_on="codigo_municipio")

    df = df.merge(dim_ubicacion,
                  left_on="id_y",
                  right_on="municipio_id")

    df = df.merge(dim_programa_oferta,
                  left_on=["id_x", "id_y_y", "id"],
                  right_on=["programa_id", "institucion_id", "ubicacion_oferta_id"])

    df = df.merge(dim_sexo,
                  left_on="sexo",
                  right_on="descripcion",
                  how="left")

    df = df.merge(dim_tiempo,
                  left_on=["ano", "semestre"],
                  right_on=["anio", "semestre"])

    # ---- Selección final ----

    df_final = df[[
        "id",          # programa_oferta_id
        "id_y",        # tiempo_id
        "id_sexo",     # sexo_id
        "inscritos"
    ]].rename(columns={
        "id": "programa_oferta_id",
        "id_y": "tiempo_id",
        "id_sexo": "sexo_id",
        "inscritos": "cantidad"
    })

    print("Insertando registros en tb_fact_inscritos...")

    df_final.to_sql(
        "tb_fact_inscritos",
        engine,
        if_exists="append",
        index=False,
        method="multi"
    )

    print("Carga completada correctamente.")
