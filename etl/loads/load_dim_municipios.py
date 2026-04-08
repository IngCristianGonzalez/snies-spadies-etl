import pandas as pd

def load_dim_municipios(engine, df):

    print("🚀 Cargando dimensión municipio...")

    df_mun = df[[
        "codigo_municipio",
        "nombre",
        "departamento_id"
    ]].drop_duplicates()

    # 🔹 limpieza
    df_mun["codigo_municipio"] = df_mun["codigo_municipio"].astype(str).str.strip()
    df_mun["nombre"] = df_mun["nombre"].astype(str).str.strip().str.upper()
    df_mun["departamento_id"] = df_mun["departamento_id"].astype(int)

    # ---- evitar duplicados ----
    existentes = pd.read_sql(
        "SELECT codigo_municipio FROM tb_dim_municipio",
        engine
    )

    df_mun = df_mun[
        ~df_mun["codigo_municipio"].isin(existentes["codigo_municipio"])
    ]

    print("📊 Nuevos municipios:", len(df_mun))

    if len(df_mun) > 0:
        df_mun.to_sql(
            "tb_dim_municipio",
            engine,
            if_exists="append",
            index=False,
            method="multi"
        )
        print("✅ Dimensión municipio cargada")
    else:
        print("⚠️ No hay nuevos municipios")