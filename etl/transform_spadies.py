import pandas as pd
import re

def limpiar_nombre_variable(nombre_archivo):
    nombre = nombre_archivo.upper()
    nombre = nombre.replace(".CSV", "")
    nombre = re.sub(r'PORCENTAJES', '', nombre)
    nombre = re.sub(r'\(\d+\)', '', nombre)
    return nombre.strip()


def transformar_spadies(df, nombre_archivo):

    # 🔹 Limpiar BOM / encoding raro
    df.columns = df.columns.str.replace('ï»¿', '')

    # 🔹 Detectar columna de categoría
    col_categoria = df.columns[0]

    # 🔹 Nombre de variable
    variable = limpiar_nombre_variable(nombre_archivo)

    # 🔹 UNPIVOT
    df_long = df.melt(
        id_vars=[col_categoria],
        var_name="periodo",
        value_name="porcentaje"
    )

    # 🔹 Limpiar nulos
    df_long = df_long.dropna()

    # 🔹 Limpiar porcentaje
    df_long["porcentaje"] = df_long["porcentaje"].astype(str).str.replace('%', '')
    df_long["porcentaje"] = pd.to_numeric(df_long["porcentaje"], errors='coerce')

    # 🔹 Limpiar periodo (espacios)
    df_long["periodo"] = df_long["periodo"].astype(str).str.replace(" ", "")

    # 🔹 Separar año y semestre
    df_long["anio"] = df_long["periodo"].str.split('-').str[0].astype(int)
    df_long["semestre"] = df_long["periodo"].str.split('-').str[1].astype(int)

    # 🔥 FILTRO CLAVE
    df_long = df_long[df_long["anio"] >= 2015]

    # 🔹 Renombrar categoría
    df_long = df_long.rename(columns={
        col_categoria: "categoria"
    })

    # 🔹 Limpiar textos raros
    df_long["categoria"] = df_long["categoria"].astype(str)
    df_long["categoria"] = df_long["categoria"].str.replace("¿", "ó")

    # 🔹 Agregar variable
    df_long["variable"] = variable

    return df_long[["variable", "categoria", "anio", "semestre", "porcentaje"]]