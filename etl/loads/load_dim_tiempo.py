import pandas as pd

def load_dim_tiempo(engine, df):

    print("🚀 Cargando dimensión tiempo...")

    df_tiempo = df[["anio", "semestre"]].drop_duplicates()

    # 🔹 limpieza
    df_tiempo["anio"] = df_tiempo["anio"].astype(int)
    df_tiempo["semestre"] = df_tiempo["semestre"].astype(int)

    # ---- evitar duplicados ----
    existentes = pd.read_sql(
        "SELECT anio, semestre FROM tb_dim_tiempo",
        engine
    )

    df_tiempo = df_tiempo.merge(
        existentes,
        on=["anio", "semestre"],
        how="left",
        indicator=True
    )

    df_tiempo = df_tiempo[df_tiempo["_merge"] == "left_only"].drop(columns=["_merge"])

    print("📊 Nuevos periodos:", len(df_tiempo))

    if len(df_tiempo) > 0:
        df_tiempo.to_sql(
            "tb_dim_tiempo",
            engine,
            if_exists="append",
            index=False,
            method="multi"
        )
        print("✅ Dimensión tiempo cargada")
    else:
        print("⚠️ No hay nuevos periodos")