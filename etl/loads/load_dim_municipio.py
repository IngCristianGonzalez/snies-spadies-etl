def load_dim_departamento(engine, df):

    df_dep = df[[
        "codigo_del_departamento_programa",
        "departamento_de_oferta_del_programa"
    ]].drop_duplicates()

    df_dep = df_dep.rename(columns={
        "codigo_del_departamento_programa": "codigo_departamento",
        "departamento_de_oferta_del_programa": "nombre"
    })

    df_dep.to_sql(
        "tb_dim_departamento",
        engine,
        if_exists="append",
        index=False,
        method="multi"
    )