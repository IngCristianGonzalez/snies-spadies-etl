def validate_inscritos(df):

    print("\n===== VALIDACIÓN INSCRITOS =====")

    print("Registros totales:", len(df))

    print("\nAños encontrados:")
    print(df["anio"].unique())

    print("\nSemestres encontrados:")
    print(df["semestre"].unique())

    print("\nInstituciones encontradas:")
    print(df["codigo_de_la_institucion"].unique())

    print("\nValores únicos sexo:")
    print(df["sexo"].unique())

    print("\nTotal inscritos:", df["inscritos"].sum())

    # Duplicados por grano
    duplicates = df.duplicated(
        subset=[
            "codigo_de_la_institucion",
            "codigo_snies_del_programa",
            "codigo_del_municipio_programa",
            "sexo",
            "anio",
            "semestre"
        ]
    )

    if duplicates.any():
        print("\n⚠ Hay duplicados en el grano esperado.")
    else:
        print("\n✔ No hay duplicados en el grano esperado.")
