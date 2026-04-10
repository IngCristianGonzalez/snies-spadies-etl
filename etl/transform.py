import unicodedata
import pandas as pd

# 🔹 Normalizar nombres de columnas
def normalize_column_name(col):
    col = col.replace("\n", " ")
    col = col.strip().lower()
    col = unicodedata.normalize("NFKD", col)
    col = col.encode("ascii", "ignore").decode("utf-8")
    col = col.replace(" ", "_")
    col = col.replace("(", "").replace(")", "")
    return col


# 🔹 Limpieza general SNIES
def clean_snies(df):
    df.columns = [normalize_column_name(c) for c in df.columns]
    return df


# 🔹 Detectar columna dinámica (valor)
def detectar_columna_valor(df):
    patrones = {
        "inscritos": ["inscrit", "inscripcion", "inscripciones"],
        "admitidos": ["admit", "admision", "admisiones"],
        "matriculados": ["matricul", "matricula", "matriculas"]
    }

    for col in df.columns:
        col_clean = col.lower().replace("_", "").replace(" ", "")
        for tipo, keywords in patrones.items():
            if any(k in col_clean for k in keywords):
                return col, tipo

    print("⚠️ Columnas disponibles:", df.columns.tolist())
    raise ValueError("No se encontró columna de valor")



# 🔹 Filtrar instituciones
def filter_instituciones(df, codigos=["1120", "1123"]):
    df["codigo_de_la_institucion"] = df["codigo_de_la_institucion"].astype(str).str.strip()
    df = df[df["codigo_de_la_institucion"].isin(codigos)]
    print("🏫 Instituciones:", df["codigo_de_la_institucion"].unique())
    print("📊 Registros:", len(df))
    return df


# 🔹 Mapear columnas dinámicamente
def mapear_columnas(df):
    columnas = {}

    for col in df.columns:
        c = col.lower().replace("\n", " ").strip()

        # institución
        if "codigo" in c and "institucion" in c:
            columnas["codigo_de_la_institucion"] = col

        # programa snies
        elif "codigo" in c and "snies" in c and "programa" in c:
            columnas["codigo_snies_del_programa"] = col

        # municipio del programa
        elif "codigo" in c and "municipio" in c and "programa" in c:
            columnas["codigo_del_municipio_programa"] = col

        # id género (puede venir como id_sexo, id genero, etc.)
        elif any(x in c for x in ["idgenero", "id_genero", "id género", "idsexo", "id_sexo", "id sexo"]):
            columnas["id_genero"] = col
            # Normalizar valores: convertir a numérico, reemplazar texto y ceros por 3
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(3).astype(int)
            df[col] = df[col].replace(0, 3)


        # fallback género sin id
        elif "genero" in c and "id" not in c:
            columnas["genero"] = col

        # año
        elif "ano" in c or "año" in c:
            columnas["anio"] = col

        # semestre
        elif "semestre" in c:
            columnas["semestre"] = col

    return columnas


# 🔥 TRANSFORMADOR FINAL
def transformar_snies(df):
    # 1. limpiar columnas
    df = clean_snies(df)

    # 2. detectar valor
    col_valor, tipo = detectar_columna_valor(df)
    df = df.rename(columns={col_valor: "valor"})
    df["tipo"] = tipo

    # 3. mapear columnas
    mapeo = mapear_columnas(df)

    required = [
        "codigo_de_la_institucion",
        "codigo_snies_del_programa",
        "codigo_del_municipio_programa",
        "anio",
        "semestre"
    ]

    if "id_genero" in mapeo:
        required.append("id_genero")
    else:
        required.append("genero")

    missing = [c for c in required if c not in mapeo]
    if missing:
        print("⚠️ Columnas disponibles:", df.columns.tolist())
        print("🔍 Mapeo detectado:", mapeo)
        raise ValueError(f"❌ Faltan columnas: {missing}")

    # 4. renombrar columnas
    rename_dict = {
        mapeo["codigo_de_la_institucion"]: "codigo_de_la_institucion",
        mapeo["codigo_snies_del_programa"]: "codigo_snies_del_programa",
        mapeo["codigo_del_municipio_programa"]: "codigo_del_municipio_programa",
        mapeo["anio"]: "anio",
        mapeo["semestre"]: "semestre"
    }

    if "id_genero" in mapeo:
        rename_dict[mapeo["id_genero"]] = "id_genero"
    else:
        rename_dict[mapeo["genero"]] = "genero"

    df = df.rename(columns=rename_dict)

    # 5. tipos de datos
    df["codigo_de_la_institucion"] = df["codigo_de_la_institucion"].astype(str).str.strip()
    df["codigo_snies_del_programa"] = df["codigo_snies_del_programa"].astype(str).str.strip()
    df["codigo_del_municipio_programa"] = df["codigo_del_municipio_programa"].astype(str).str.strip().str.zfill(5)
    df = df.dropna(subset=["anio"])
    df["anio"] = df["anio"].astype(int)
    df["semestre"] = df["semestre"].astype(int)
    df["valor"] = df["valor"].fillna(0).astype(int)

    if "id_genero" in df.columns:
        df["id_genero"] = df["id_genero"].astype(int)
    else:
        df["genero"] = df["genero"].astype(str).str.upper().str.strip()

    # 6. filtrar instituciones
    df = filter_instituciones(df)

    # 7. columnas finales
    columnas_finales = [
        "codigo_de_la_institucion",
        "codigo_snies_del_programa",
        "codigo_del_municipio_programa",
        "anio",
        "semestre",
        "tipo",
        "valor"
    ]

    if "id_genero" in df.columns:
        columnas_finales.append("id_genero")
    else:
        columnas_finales.append("genero")

    df = df[columnas_finales]
    return df
