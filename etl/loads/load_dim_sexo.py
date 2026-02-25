def load_dim_sexos(engine, df):

    df_tiempo = df[["descripcion"]].drop_duplicates()

    df_tiempo = df_tiempo.rename(columns={
        "descripcion": "descripcion"
    })

    df_tiempo.to_sql(
        "tb_dim_sexo",
        engine,
        if_exists="append",
        index=False,
        method="multi"
    )
