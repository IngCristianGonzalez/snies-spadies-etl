# etl/validate/validate.py

def validate_snies_generic(df):
    """Función genérica que valida cualquier tipo (inscritos/admitidos/matriculados/graduados)"""
    
    print("\n===== VALIDACIÓN SNIES =====")
    print("Registros totales:", len(df))
    
    print("\nAños encontrados:")
    print(df["anio"].unique())
    
    print("\nSemestres encontrados:")
    print(df["semestre"].unique())
    
    print("\nInstituciones encontradas:")
    print(df["codigo_de_la_institucion"].unique())
    
    print("\nValores únicos género:")
    print(df["id_genero"].unique())
    
    print("\nTipos encontrados:")
    print(df["tipo"].unique())
    
    print("\nTotal valor:", df["valor"].sum())
    
    print("\nTotales por tipo:")
    print(df.groupby("tipo")["valor"].sum())
    
    # Duplicados
    duplicates = df.duplicated(
        subset=[
            "codigo_de_la_institucion",
            "codigo_snies_del_programa",
            "codigo_del_municipio_programa",
            "id_genero",
            "anio",
            "semestre",
            "tipo"
        ]
    )
    
    if duplicates.any():
        print("\n⚠ Hay duplicados en el grano esperado.")
    else:
        print("\n✔ No hay duplicados en el grano esperado.")