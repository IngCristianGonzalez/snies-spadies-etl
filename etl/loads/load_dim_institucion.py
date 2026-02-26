def load_dim_institucion(engine, df):

    df_inst = df[[
        "codigo_ies",
        "nombre",
        "sector",
        "caracter",
        "departamento_id",
        "municipio_id"

    ]].drop_duplicates()

    

    df_inst.to_sql(
        "tb_dim_institucion",
        engine,
        if_exists="append",
        index=False,
        method="multi"
    )