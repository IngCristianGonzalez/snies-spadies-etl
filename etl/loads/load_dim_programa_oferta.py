def load_dim_programa_oferta(engine, df):

    print("🚀 Cargando programa_oferta...")

    import pandas as pd

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

    # 🔥 NORMALIZAR
    df["codigo_snies_del_programa"] = df["codigo_snies_del_programa"].astype(str).str.strip()
    df["codigo_de_la_institucion"] = df["codigo_de_la_institucion"].astype(str).str.strip()
    df["codigo_del_municipio_programa"] = df["codigo_del_municipio_programa"].astype(str).str.zfill(5)

    dim_programa["codigo_snies_del_programa"] = dim_programa["codigo_snies_del_programa"].astype(str).str.strip()
    dim_institucion["codigo_ies"] = dim_institucion["codigo_ies"].astype(str).str.strip()
    dim_municipio["codigo_municipio"] = dim_municipio["codigo_municipio"].astype(str).str.zfill(5)

    # 🔥 MERGES
    df = df.merge(dim_programa, on="codigo_snies_del_programa")
    df = df.rename(columns={"id": "programa_id"})

    df = df.merge(dim_institucion, left_on="codigo_de_la_institucion", right_on="codigo_ies")
    df = df.rename(columns={"id": "institucion_id"})

    df = df.merge(dim_municipio, left_on="codigo_del_municipio_programa", right_on="codigo_municipio")
    df = df.rename(columns={"id": "municipio_id"})

    df_final = df[[
        "programa_id",
        "institucion_id",
        "municipio_id"
    ]].drop_duplicates()

    print("📊 combinaciones únicas:", len(df_final))

    # 🔥 EVITAR DUPLICADOS
    existentes = pd.read_sql("""
        SELECT programa_id, institucion_id, municipio_id
        FROM tb_dim_programa_oferta
    """, engine)

    df_final = df_final.merge(
        existentes,
        on=["programa_id", "institucion_id", "municipio_id"],
        how="left",
        indicator=True
    )

    df_final = df_final[df_final["_merge"] == "left_only"]
    df_final = df_final.drop(columns=["_merge"])

    print("📊 nuevos a insertar:", len(df_final))

    if len(df_final) > 0:
        df_final.to_sql(
            "tb_dim_programa_oferta",
            engine,
            if_exists="append",
            index=False
        )
        print("✅ programa_oferta cargado")
    else:
        print("⚠️ No hay nuevos registros")