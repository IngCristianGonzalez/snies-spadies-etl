def load_dim_municipios(engine, df):

    df_inst = df[[
        "codigo_municipio",
        "nombre",
        "departamento_id"
    ]]

   

    df_inst.to_sql(
        "tb_dim_municipio",
        engine,
        if_exists="append",
        index=False,
        method="multi"
    )