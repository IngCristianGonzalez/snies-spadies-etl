import pandas as pd
from sqlalchemy import text

def load_fact_snies(engine, df, tipo):
    """
    Carga los datos de hechos SNIES con normalización CONSISTENTE
    """
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
    
    # 🔥 NORMALIZACIÓN CONSISTENTE (IGUAL QUE EN load_dim_programa_oferta)
    df_tipo["codigo_snies_del_programa"] = df_tipo["codigo_snies_del_programa"].astype(str).str.strip()
    df_tipo["codigo_de_la_institucion"] = df_tipo["codigo_de_la_institucion"].astype(str).str.strip()
    df_tipo["codigo_del_municipio_programa"] = df_tipo["codigo_del_municipio_programa"].astype(str).str.strip()  # SIN zfill
    
    dim_programa["codigo_snies_del_programa"] = dim_programa["codigo_snies_del_programa"].astype(str).str.strip()
    dim_institucion["codigo_ies"] = dim_institucion["codigo_ies"].astype(str).str.strip()
    dim_municipio["codigo_municipio"] = dim_municipio["codigo_municipio"].astype(str).str.strip()  # SIN zfill
    
    dim_programa = dim_programa.drop_duplicates(subset=["codigo_snies_del_programa"])
    dim_institucion = dim_institucion.drop_duplicates(subset=["codigo_ies"])
    dim_municipio = dim_municipio.drop_duplicates(subset=["codigo_municipio"])
    
    print(f"\n📊 Datos origen:")
    print(f"   Instituciones en df: {df_tipo['codigo_de_la_institucion'].unique()}")
    print(f"   Municipios en df: {df_tipo['codigo_del_municipio_programa'].unique()}")
    print(f"   Programas únicos en df: {df_tipo['codigo_snies_del_programa'].unique()}")
    print(f"   Programas únicos en dim_programa: {dim_programa['codigo_snies_del_programa'].unique()}")
    
    # ---- MERGES CON SUFIJOS PARA EVITAR CONFLICTOS ----
    
    # Merge con programa - USAR LEFT JOIN para NO perder registros
    df_antes = len(df_tipo)
    df_tipo = df_tipo.merge(
        dim_programa, 
        on="codigo_snies_del_programa", 
        how="left",  # ← LEFT JOIN, no INNER
        suffixes=("", "_programa")
    )
    # Renombrar solo si existe la columna id
    if "id" in df_tipo.columns:
        df_tipo = df_tipo.rename(columns={"id": "programa_id"})
    
    registros_perdidos = df_antes - len(df_tipo)
    print(f"\n✅ Merge programa (LEFT): {df_antes} → {len(df_tipo)} registros")
    if registros_perdidos > 0:
        print(f"   ⚠️ Se perdieron {registros_perdidos} registros")
    
    # Verificar cuántos tienen programa_id = NaN (programas no encontrados)
    sin_programa = df_tipo[df_tipo["programa_id"].isna()]
    if len(sin_programa) > 0:
        print(f"   📊 {len(sin_programa)} registros SIN programa_id válido (no en dim_programa)")
        print(f"      Estos se filtrarán después")
    
    # Merge con institución - CON SUFIJOS
    df_antes = len(df_tipo)
    print(f"\n📊 Antes merge institución:")
    print(f"   institucion_id únicos EN DF: {sorted(df_tipo['codigo_de_la_institucion'].unique())}")
    print(f"   codigo_ies EN DIM: {sorted(dim_institucion['codigo_ies'].unique())}")
    
    df_tipo = df_tipo.merge(
        dim_institucion, 
        left_on="codigo_de_la_institucion", 
        right_on="codigo_ies",
        suffixes=("", "_institucion")
    )
    df_tipo = df_tipo.rename(columns={"id": "institucion_id"})
    print(f"✅ Merge institución: {df_antes} → {len(df_tipo)} registros")
    print(f"   institucion_id únicos: {sorted(df_tipo['institucion_id'].unique())}")
    print(f"   Conteo: {df_tipo['institucion_id'].value_counts().sort_index().to_dict()}")
    
    # Merge con municipio - CON SUFIJOS
    df_antes = len(df_tipo)
    df_tipo = df_tipo.merge(
        dim_municipio, 
        left_on="codigo_del_municipio_programa", 
        right_on="codigo_municipio",
        suffixes=("", "_municipio")
    )
    df_tipo = df_tipo.rename(columns={"id": "municipio_id"})
    print(f"✅ Merge municipio: {df_antes} → {len(df_tipo)} registros")
    
    # 🔥 FILTRAR registros sin programa_id válido
    registros_sin_programa = len(df_tipo[df_tipo["programa_id"].isna()])
    if registros_sin_programa > 0:
        print(f"\n⚠️ {registros_sin_programa} registros sin programa_id válido")
        print("   (programas no encontrados en tb_dim_programa)")
        sin_prog = df_tipo[df_tipo["programa_id"].isna()]
        print(f"   institucion_id en registros sin programa: {sin_prog['codigo_de_la_institucion'].unique()}")
        df_tipo = df_tipo[df_tipo["programa_id"].notna()].copy()
        print(f"   Registros después filtrar: {len(df_tipo)}")
    
    print(f"\n✅ Después filtrar por programa_id:")
    print(f"   institucion_id únicos: {sorted(df_tipo['institucion_id'].unique())}")
    
    # Guardar institucion_id ANTES de otros merges
    df_tipo_institucion = df_tipo[["institucion_id"]].copy()
    
    print(f"\n✅ Antes merge programa_oferta:")
    print(f"   institucion_id únicos: {sorted(df_tipo['institucion_id'].unique())}")
    
    # Merge con programa_oferta
    df_antes = len(df_tipo)
    df_tipo = df_tipo.merge(
        dim_programa_oferta[["id", "programa_id", "institucion_id", "municipio_id"]],
        on=["programa_id", "institucion_id", "municipio_id"],
        how="left",
        suffixes=("", "_oferta")
    )
    df_tipo = df_tipo.rename(columns={"id": "programa_oferta_id"})
    print(f"✅ Merge programa_oferta: {df_antes} → {len(df_tipo)} registros")
    print(f"   institucion_id únicos: {sorted(df_tipo['institucion_id'].unique())}")
    
    # Merge con tiempo
    df_antes = len(df_tipo)
    df_tipo = df_tipo.merge(dim_tiempo, on=["anio", "semestre"], suffixes=("", "_tiempo"))
    df_tipo = df_tipo.rename(columns={"id": "tiempo_id"})
    print(f"✅ Merge tiempo: {df_antes} → {len(df_tipo)} registros")
    
    # Restaurar institucion_id (por seguridad)
    df_tipo["institucion_id"] = df_tipo_institucion["institucion_id"].values
    
    # Filtrar solo registros con programa_oferta_id válido
    registros_sin_oferta = len(df_tipo[df_tipo["programa_oferta_id"].isna()])
    if registros_sin_oferta > 0:
        print(f"\n⚠️ {registros_sin_oferta} registros sin programa_oferta válido")
        print("  Combinaciones no encontradas en dim_programa_oferta:")
        sin_oferta = df_tipo[df_tipo["programa_oferta_id"].isna()]
        print(f"  institucion_id en registros SIN oferta: {sin_oferta['institucion_id'].unique()}")
        for idx, row in sin_oferta[["programa_id", "institucion_id", "municipio_id"]].drop_duplicates().iterrows():
            print(f"    programa_id={row['programa_id']}, institucion_id={row['institucion_id']}, municipio_id={row['municipio_id']}")
        df_tipo = df_tipo[df_tipo["programa_oferta_id"].notna()].copy()
    
    print(f"\n✅ Después filtrar por programa_oferta_id:")
    print(f"   institucion_id únicos: {sorted(df_tipo['institucion_id'].unique())}")
    print(f"   Conteo: {df_tipo['institucion_id'].value_counts().sort_index().to_dict()}")
    
    # ---- DATA FINAL ----
    df_final = df_tipo[[
        "programa_oferta_id",
        "institucion_id",
        "tiempo_id",
        "id_genero",
        "tipo",
        "valor"
    ]].rename(columns={
        "id_genero": "genero_id",
        "valor": "cantidad"
    })
    
    print(f"\n📊 En df_final:")
    print(f"   institucion_id únicos: {sorted(df_final['institucion_id'].unique())}")
    print(f"   Conteo: {df_final['institucion_id'].value_counts().sort_index().to_dict()}")
    
    # ---- VERIFICAR DUPLICADOS EN BD ----
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
        
        print(f"\n📊 Registros a insertar (nuevos): {len(df_final)}")
    
    # ---- INSERT ----
    if len(df_final) > 0:
        df_final.to_sql(
            "tb_fact_snies",   
            engine,
            if_exists="append",
            index=False,
            method="multi"
        )
        print(f"\n✅ Carga completada para {tipo}")
        
        # REPORTE FINAL
        print(f"\n{'='*60}")
        print(f"📊 REPORTE FINAL {tipo.upper()}")
        print(f"{'='*60}")
        print(f"Total registros insertados: {len(df_final)}")
        print(f"Instituciones cargadas: {sorted(df_final['institucion_id'].unique())}")
        print(f"Registros por institución:")
        for inst_id in sorted(df_final['institucion_id'].unique()):
            count = len(df_final[df_final['institucion_id'] == inst_id])
            print(f"  institucion_id={inst_id}: {count} registros")
        print(f"{'='*60}\n")
    else:
        print(f"\n⚠️ No hay registros nuevos para {tipo}\n")