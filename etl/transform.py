def clean_snies(df):
    df.columns = df.columns.str.lower().str.strip()

    if "sexo" in df.columns:
        df["sexo"] = df["sexo"].str.upper().str.strip()
        df["sexo"] = df["sexo"].fillna("NO REPORTADO")

    df = df.fillna("")
    return df
