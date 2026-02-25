import unicodedata
#Normalizar columnas
def normalize_column_name(col):
    col = col.strip().lower()
    col = unicodedata.normalize("NFKD", col)
    col = col.encode("ascii", "ignore").decode("utf-8")
    col = col.replace(" ", "_")
    return col

#Normalizacion de sexos
def clean_snies(df):
    df.columns = [normalize_column_name(c) for c in df.columns]

    if "sexo" in df.columns:
        df["sexo"] = df["sexo"].astype(str).str.upper().str.strip()
        df["sexo"] = df["sexo"].fillna("NO REPORTADO")

    return df

# Filtrado por instituciones
def filter_instituciones(df, codigos=["1120", "1123"]):

    if "codigo_de_la_institucion" not in df.columns:
        raise ValueError("No se encontró la columna codigo_de_la_institucion")

    df["codigo_de_la_institucion"] = (
        df["codigo_de_la_institucion"]
        .astype(str)
        .str.strip()
    )

    df_filtrado = df[
        df["codigo_de_la_institucion"].isin(codigos)
    ]

    print("Instituciones encontradas:", df_filtrado["codigo_de_la_institucion"].unique())
    print("Registros después del filtro:", len(df_filtrado))

    return df_filtrado

# Normalizar matriculados 

def clean_matriculados(df):

    df = clean_snies(df)

    required_cols = [
        "codigo_de_la_institucion",
        "codigo_snies_del_programa",
        "codigo_del_municipio_programa",
        "sexo",
        "ano",
        "semestre",
        "matriculados"
    ]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {missing}")

    # Tipos correctos
    df["codigo_de_la_institucion"] = df["codigo_de_la_institucion"].astype(str).str.strip()
    df["codigo_snies_del_programa"] = df["codigo_snies_del_programa"].astype(str).str.strip()
    df["codigo_del_municipio_programa"] = df["codigo_del_municipio_programa"].astype(str).str.strip()

    df["sexo"] = df["sexo"].astype(str).str.upper().str.strip()
    df["ano"] = df["ano"].astype(int)
    df["semestre"] = df["semestre"].astype(int)
    df["matriculados"] = df["matriculados"].fillna(0).astype(int)

    return df


#Limpiar inscritos 
def clean_inscritos(df):

    df = clean_snies(df)

    required_cols = [
        "codigo_de_la_institucion",
        "codigo_snies_del_programa",
        "codigo_del_municipio_programa",
        "sexo",
        "anio",
        "semestre",
        "inscritos"
    ]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {missing}")

    # Normalización tipos
    df["codigo_de_la_institucion"] = df["codigo_de_la_institucion"].astype(str).str.strip()
    df["codigo_snies_del_programa"] = df["codigo_snies_del_programa"].astype(str).str.strip()
    df["codigo_del_municipio_programa"] = df["codigo_del_municipio_programa"].astype(str).str.strip()

    df["sexo"] = df["sexo"].astype(str).str.upper().str.strip()
    df["anio"] = df["anio"].astype(str)
    df["semestre"] = df["semestre"].astype(str)
    df["inscritos"] = df["inscritos"].fillna(0).astype(str)

    return df
