import pandas as pd

def limpiar_codigo(col):
    return (
        col.astype(str)
        .str.replace('.0', '', regex=False)
        .str.strip()
    )

def load_dim_programa_oferta(engine, df):
    """
    Carga todas las combinaciones únicas de programa + institución + municipio
    """
    print("🚀 Cargando tb_dim_programa_oferta...")

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

    # 🔥 NORMALIZAR - CONSISTENCIA ES CLAVE
    df["codigo_snies_del_programa"] = limpiar_codigo(df["codigo_snies_del_programa"])
    df["codigo_de_la_institucion"] = limpiar_codigo(df["codigo_de_la_institucion"])
    df["codigo_del_municipio_programa"] = limpiar_codigo(df["codigo_del_municipio_programa"])
    df = df[df["codigo_del_municipio_programa"] != "000"]
    dim_programa["codigo_snies_del_programa"] = limpiar_codigo(dim_programa["codigo_snies_del_programa"])
    dim_institucion["codigo_ies"] = limpiar_codigo(dim_institucion["codigo_ies"])
    dim_municipio["codigo_municipio"] = limpiar_codigo(dim_municipio["codigo_municipio"])  # SIN zfill aquí
    
    print(f"\n📊 Antes de merges:")
    print(f"   df instituciones: {df['codigo_de_la_institucion'].unique()}")
    print(f"   dim_institucion: {dim_institucion['codigo_ies'].unique()}")
    print(f"   df municipios: {df['codigo_del_municipio_programa'].unique()}")
    print(f"   dim_municipio: {dim_municipio['codigo_municipio'].unique()}")

    # 🔥 MERGE 1: Programa
    df_antes = len(df)
    df = df.merge(dim_programa, on="codigo_snies_del_programa", suffixes=("", "_programa"))
    df = df.rename(columns={"id": "programa_id"})
    print(f"\n✅ Merge programa: {df_antes} → {len(df)} registros")

    # 🔥 MERGE 2: Institución - CON SUFIJOS
    df_antes = len(df)
    df = df.merge(
        dim_institucion, 
        left_on="codigo_de_la_institucion", 
        right_on="codigo_ies",
        suffixes=("", "_institucion")
    )
    df = df.rename(columns={"id": "institucion_id"})
    print(f"✅ Merge institución: {df_antes} → {len(df)} registros")
    print(f"   institucion_id únicos: {df['institucion_id'].unique()}")

    # 🔥 MERGE 3: Municipio - CON SUFIJOS
    df_antes = len(df)
    df = df.merge(
        dim_municipio, 
        left_on="codigo_del_municipio_programa", 
        right_on="codigo_municipio",
        suffixes=("", "_municipio")
    )
    df = df.rename(columns={"id": "municipio_id"})
    print(f"✅ Merge municipio: {df_antes} → {len(df)} registros")

    # Seleccionar solo las columnas necesarias y quitar duplicados
    df_final = df[[
        "programa_id",
        "institucion_id",
        "municipio_id"
    ]].drop_duplicates()

    print(f"\n📊 Combinaciones únicas: {len(df_final)}")
    print(f"   institucion_id en combinaciones: {df_final['institucion_id'].unique()}")

    # 🔥 EVITAR DUPLICADOS
    existentes = pd.read_sql("""
        SELECT programa_id, institucion_id, municipio_id
        FROM tb_dim_programa_oferta
    """, engine)

    print(f"\n📊 Registros existentes en BD: {len(existentes)}")
    print(f"   institucion_id existentes: {existentes['institucion_id'].unique()}")

    df_final = df_final.merge(
        existentes,
        on=["programa_id", "institucion_id", "municipio_id"],
        how="left",
        indicator=True
    )

    df_final = df_final[df_final["_merge"] == "left_only"]
    df_final = df_final.drop(columns=["_merge"])

    print(f"\n📊 Nuevos a insertar: {len(df_final)}")
    if len(df_final) > 0:
        print(f"   institucion_id a insertar: {df_final['institucion_id'].unique()}")

    # 🔥 INSERT
    if len(df_final) > 0:
        df_final.to_sql(
            "tb_dim_programa_oferta",
            engine,
            if_exists="append",
            index=False
        )
        print("✅ tb_dim_programa_oferta cargado correctamente\n")
    else:
        print("⚠️ No hay nuevos registros para insertar\n")