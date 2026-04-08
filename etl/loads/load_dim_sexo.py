import pandas as pd

def load_dim_sexos(engine, df):

    print("🚀 Cargando dimensión sexo...")

    df_sexo = df[["codigo", "descripcion"]].drop_duplicates()

    # 🔹 limpieza
    df_sexo["codigo"] = df_sexo["codigo"].astype(int)
    df_sexo["descripcion"] = df_sexo["descripcion"].astype(str).str.strip().str.upper()

    # ---- evitar duplicados ----
    existentes = pd.read_sql(
        "SELECT codigo FROM tb_dim_sexo",
        engine
    )

    df_sexo = df_sexo[
        ~df_sexo["codigo"].isin(existentes["codigo"])
    ]

    print("📊 Nuevos sexos:", len(df_sexo))

    if len(df_sexo) > 0:
        df_sexo.to_sql(
            "tb_dim_sexo",
            engine,
            if_exists="append",
            index=False,
            method="multi"
        )
        print("✅ Dimensión sexo cargada")
    else:
        print("⚠️ No hay nuevos sexos")