def load_dim_departamentos(engine, df):

    df_inst = df[[
        "codigo_departamento",
        "nombre"
    ]]
    df_inst.to_sql(
        "tb_dim_departamento",
        engine,
        if_exists="append",
        index=False,
        method="multi"
    )