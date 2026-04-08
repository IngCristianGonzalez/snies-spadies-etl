import pandas as pd

def load_dim_programas(engine, df):

    print("🚀 Cargando dimensión programa...")

    df_prog = df[[
        "codigo_snies_del_programa",
        "nombre",
        "nivel_formacion"
    ]].drop_duplicates()

    # 🔹 renombrar columnas
    df_prog = df_prog.rename(columns={
        "codigo_snies_del_programa": "codigo_snies_del_programa",
        "nombre": "nombre",
        "nivel_formacion": "nivel_formacion"
    })

    print("📊 Nuevos programas a insertar:", len(df_prog))

    if len(df_prog) > 0:
        df_prog.to_sql(
            "tb_dim_programa",
            engine,
            if_exists="append",
            index=False,
            method="multi"
        )
        print("✅ Dimensión programa cargada")
    else:
        print("⚠️ No hay nuevos programas para insertar")