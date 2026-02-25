def load_dim_tiempo(engine, df):

    df_tiempo = df[["anio", "semestre"]].drop_duplicates()

    df_tiempo = df_tiempo.rename(columns={
        "ano": "anio"
    })

    df_tiempo.to_sql(
        "tb_dim_tiempo",
        engine,
        if_exists="append",
        index=False,
        method="multi"
    )
