import pandas as pd

def load_fact_inscritos(engine, df):

    print("🚀 Preparando carga de fact_snies...")

    # ---- DIMENSIONES ----
    dim_programa = pd.read_sql(
        "SELECT id, codigo_snies_del_programa FROM tb_dim_programa",
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

    # dim_ubicacion = pd.read_sql(
    #     "SELECT id, municipio_id FROM tb_dim_ubicacion_oferta",
    #     engine
    # )

    dim_programa_oferta = pd.read_sql("""
        SELECT id, programa_id, institucion_id
        FROM tb_dim_programa_oferta
    """, engine)

    dim_tiempo = pd.read_sql(
        "SELECT id, anio, semestre FROM tb_dim_tiempo",
        engine
    )

    # ---- MERGES LIMPIOS ----

    df = df.merge(dim_programa,
                  left_on="codigo_snies_del_programa",
                  right_on="codigo_snies_del_programa")

    df = df.rename(columns={"id": "programa_id"})

    df = df.merge(dim_institucion,
                  left_on="codigo_de_la_institucion",
                  right_on="codigo_ies")

    df = df.rename(columns={"id": "institucion_id"})

    df = df.merge(dim_municipio,
                  left_on="codigo_del_municipio_programa",
                  right_on="codigo_municipio")

    df = df.rename(columns={"id": "municipio_id"})


    df = df.merge(dim_programa_oferta,
                  on=["programa_id", "institucion_id"])

    df = df.rename(columns={"id": "programa_oferta_id"})

    df = df.merge(dim_tiempo,
                  left_on=["anio", "semestre"],
                  right_on=["anio", "semestre"])

    df = df.rename(columns={"id": "tiempo_id"})

    # ---- DATA FINAL ----

    df_final = df[[
        "programa_oferta_id",
        "tiempo_id",
        "id_genero",
        "tipo",
        "valor"
    ]].rename(columns={
        "id_genero": "genero_id",
        "valor": "cantidad"
    })

    print("📊 Registros a insertar:", len(df_final))

    # ---- INSERT ----
    df_final.to_sql(
        "tb_fact_snies",   # 🔥 NUEVA FACT
        engine,
        if_exists="append",
        index=False,
        method="multi"
    )

    print("✅ Carga completada correctamente.")