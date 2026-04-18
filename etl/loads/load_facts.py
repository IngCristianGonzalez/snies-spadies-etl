import pandas as pd
from sqlalchemy import text
def load_fact_snies(engine, df, tipo):
    print(f"🚀 Preparando carga de tb_fact_snies para {tipo}...")
    
    # Filtrar solo los registros del tipo actual
    df_tipo = df[df["tipo"] == tipo].copy()
    
    if len(df_tipo) == 0:
        print(f"⚠️ No hay registros de tipo '{tipo}' en el archivo")
        return
    
    print(f"📊 Registros para {tipo}: {len(df_tipo)}")
    
    # ---- DIMENSIONES ----
    dim_programa = pd.read_sql(
        "SELECT id, codigo_snies_del_programa FROM tb_dim_programa",
        engine
    )
    dim_institucion = pd.read_sql(
        "SELECT id, codigo_ies FROM tb_dim_institucion",
        engine
    )
    dim_municipio = pd.read_sql(
        "SELECT id, codigo_municipio FROM tb_dim_municipio",
        engine
    )
    dim_programa_oferta = pd.read_sql("""
        SELECT id, programa_id, institucion_id, municipio_id
        FROM tb_dim_programa_oferta
    """, engine)
    dim_tiempo = pd.read_sql(
        "SELECT id, anio, semestre FROM tb_dim_tiempo",
        engine
    )
    
    # ---- MERGES ----
    df_tipo = df_tipo.merge(dim_programa, on="codigo_snies_del_programa")
    df_tipo = df_tipo.rename(columns={"id": "programa_id"})
    
    df_tipo = df_tipo.merge(dim_institucion, left_on="codigo_de_la_institucion", right_on="codigo_ies")
    df_tipo = df_tipo.rename(columns={"id": "institucion_id"})
    
    df_tipo = df_tipo.merge(dim_municipio, left_on="codigo_del_municipio_programa", right_on="codigo_municipio")
    df_tipo = df_tipo.rename(columns={"id": "municipio_id"})
    
    df_tipo = df_tipo.merge(dim_programa_oferta, on=["programa_id", "institucion_id", "municipio_id"])
    df_tipo = df_tipo.rename(columns={"id": "programa_oferta_id"})
    
    df_tipo = df_tipo.merge(dim_tiempo, on=["anio", "semestre"])
    df_tipo = df_tipo.rename(columns={"id": "tiempo_id"})
    
    # ---- DATA FINAL ----
    df_final = df_tipo[[
        "programa_oferta_id",
        "insitucion_id",
        "tiempo_id",
        "id_genero",
        "tipo",
        "valor"
    ]].rename(columns={
        "id_genero": "genero_id",
        "valor": "cantidad"
    })
    
    # Eliminar duplicados
    df_final = df_final.drop_duplicates(
        subset=["programa_oferta_id", "tiempo_id", "genero_id", "tipo"]
    )
    
    print("📊 Registros a insertar (sin duplicados):", len(df_final))
    
    existentes = pd.read_sql(
        text("""
            SELECT programa_oferta_id, tiempo_id, genero_id, tipo
            FROM tb_fact_snies
            WHERE tipo = :tipo
        """),
        engine,
        params={"tipo": tipo}
)
    
    if len(existentes) > 0:
        existentes["llave"] = (
            existentes["programa_oferta_id"].astype(str) + "-" +
            existentes["tiempo_id"].astype(str) + "-" +
            existentes["genero_id"].astype(str) + "-" +
            existentes["tipo"].astype(str)
        )
        
        df_final["llave"] = (
            df_final["programa_oferta_id"].astype(str) + "-" +
            df_final["tiempo_id"].astype(str) + "-" +
            df_final["genero_id"].astype(str) + "-" +
            df_final["tipo"].astype(str)
        )
        
        df_final = df_final[~df_final["llave"].isin(existentes["llave"])]
        df_final = df_final.drop(columns=["llave"])
        
        print("📊 Registros a insertar (nuevos):", len(df_final))
    
    # ---- INSERT ----
    if len(df_final) > 0:
        df_final.to_sql(
            "tb_fact_snies",   
            engine,
            if_exists="append",
            index=False,
            method="multi"
        )
        print(f"✅ Carga completada para {tipo}")
    else:
        print(f"⚠️ No hay registros nuevos para {tipo}")