import pandas as pd

def load_dim_departamentos(engine, df):

    print("🚀 Cargando dimensión departamento...")

    df_dep = df[[
        "codigo_departamento",
        "nombre"
    ]].drop_duplicates()

    # 🔹 limpieza
    df_dep["codigo_departamento"] = df_dep["codigo_departamento"].astype(str).str.strip()
    df_dep["nombre"] = df_dep["nombre"].astype(str).str.strip().str.upper()

    # ---- evitar duplicados ----
    existentes = pd.read_sql(
        "SELECT codigo_departamento FROM tb_dim_departamento",
        engine
    )

    df_dep = df_dep[
        ~df_dep["codigo_departamento"].isin(existentes["codigo_departamento"])
    ]

    print("📊 Nuevos departamentos:", len(df_dep))

    if len(df_dep) > 0:
        df_dep.to_sql(
            "tb_dim_departamento",
            engine,
            if_exists="append",
            index=False,
            method="multi"
        )
        print("✅ Dimensión departamento cargada")
    else:
        print("⚠️ No hay nuevos departamentos")