import pandas as pd

def load_dim_institucion(engine, df):

    print("🚀 Cargando dimensión institución...")

    df_inst = df[[
        "codigo_ies",
        "nombre",
        "sector",
        "caracter",
        "codigo_departamento",
        "codigo_municipio"
    ]].drop_duplicates()

    # 🔹 limpieza
    df_inst["codigo_ies"] = df_inst["codigo_ies"].astype(str).str.strip()

 # ---- traer dimensiones ----
    dim_dep = pd.read_sql(
        "SELECT id, codigo_departamento FROM tb_dim_departamento",
        engine
    )

    dim_mun = pd.read_sql(
        "SELECT id, codigo_municipio FROM tb_dim_municipio",
        engine
    )

    # 🔥 NORMALIZAR TIPOS (ESTO SOLUCIONA TODO)
    df_inst["codigo_departamento"] = df_inst["codigo_departamento"].astype(str).str.strip()
    df_inst["codigo_municipio"] = df_inst["codigo_municipio"].astype(str).str.strip()

    dim_dep["codigo_departamento"] = dim_dep["codigo_departamento"].astype(str).str.strip()
    dim_mun["codigo_municipio"] = dim_mun["codigo_municipio"].astype(str).str.strip()

    # 🔥 MERGES
    df_inst = df_inst.merge(dim_dep, on="codigo_departamento", how="left")
    df_inst = df_inst.rename(columns={"id": "departamento_id"})

    df_inst = df_inst.merge(dim_mun, on="codigo_municipio", how="left")
    df_inst = df_inst.rename(columns={"id": "municipio_id"})

    # ---- validar ----
    if df_inst["codigo_municipio"].isna().any():
        print("❌ Hay municipios que no existen en la dimensión")
        print(df_inst[df_inst["codigo_municipio"].isna()])
        raise ValueError("Municipios no encontrados")

    # ---- seleccionar final ----
    df_final = df_inst[[
        "codigo_ies",
        "nombre",
        "sector",
        "caracter",
        "departamento_id",
        "municipio_id"
    ]]

    print("📊 Nuevas instituciones:", len(df_final))

    df_final.to_sql(
        "tb_dim_institucion",
        engine,
        if_exists="append",
        index=False,
        method="multi"
    )

    print("✅ Dimensión institución cargada")