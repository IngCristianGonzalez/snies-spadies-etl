import unicodedata

def normalize_column_name(col):
    col = col.strip().lower()
    col = unicodedata.normalize("NFKD", col)
    col = col.encode("ascii", "ignore").decode("utf-8")
    col = col.replace(" ", "_")
    return col

def clean_snies(df):
    df.columns = [normalize_column_name(c) for c in df.columns]

    if "sexo" in df.columns:
        df["sexo"] = df["sexo"].astype(str).str.upper().str.strip()
        df["sexo"] = df["sexo"].fillna("NO REPORTADO")

    return df


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