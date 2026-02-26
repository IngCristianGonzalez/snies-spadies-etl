def load_dim_programa(engine, df):

    df_prog = df[[
        "codigo_snies_del_programa",
        "programa_academico",
        "nivel_de_formacion",
        "modalidad",
        "area_de_conocimiento",
        "nucleo_basico_del_conocimiento_nbc"
    ]].drop_duplicates()

    df_prog = df_prog.rename(columns={
        "codigo_snies_del_programa": "codigo_snies",
        "programa_academico": "nombre"
    })

    df_prog.to_sql(
        "tb_dim_programa",
        engine,
        if_exists="append",
        index=False,
        method="multi"
    )