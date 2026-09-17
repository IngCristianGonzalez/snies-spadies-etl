# Requirements: 016 — Lectura de dimensión tiempo

## Alcance

Leer `data/dimensions/dim_tiempo.xlsx`, localizar la hoja que contiene las
columnas `anio` y `semestre`, descartar filas introductorias, validar los datos
y devolver un `DataFrame` canónico. Esta feature no carga PostgreSQL.

## R1 [FN]

El sistema DEBE intentar leer el archivo configurado
`data/dimensions/dim_tiempo.xlsx`.

## R2 [FN]

CUANDO inspeccione el libro Excel, el sistema DEBE revisar todas sus hojas y
localizar una fila de encabezados que contenga las columnas `anio` y `semestre`,
ignorando filas introductorias anteriores.

## R3 [No deseado]

SI el archivo no existe, ENTONCES el sistema DEBE fallar con un error
descriptivo que incluya la ruta esperada y la causa.

## R4 [No deseado]

SI ninguna hoja contiene las columnas requeridas, ENTONCES el sistema DEBE
fallar indicando las hojas revisadas y las columnas encontradas.

## R5 [No deseado]

SI más de una hoja contiene las columnas requeridas, ENTONCES el sistema DEBE
fallar indicando las hojas candidatas y solicitando resolver la ambigüedad.

## R6 [FN]

CUANDO exista una única hoja candidata, el sistema DEBE seleccionar únicamente
las columnas `anio` y `semestre`, aunque existan columnas adicionales.

## R7 [FN]

CUANDO una fila contenga valores enteros o representaciones textuales de
enteros, el sistema DEBE convertir `anio` y `semestre` a tipo entero.

## R8 [FN]

CUANDO una fila tenga un `anio` entero menor o igual que cero, el sistema DEBE
omitirla y registrar la fila y el motivo del rechazo.

## R9 [FN]

CUANDO una fila tenga un `semestre` diferente de `1` o `2`, el sistema DEBE
omitirla y registrar la fila y el motivo del rechazo.

## R10 [FN]

CUANDO una fila tenga valores no convertibles a enteros, el sistema DEBE
omitirla y registrar sus valores originales y el motivo del rechazo.

## R11 [FN]

CUANDO una fila esté completamente vacía, el sistema DEBE omitirla y registrar
que fue descartada como fila vacía.

## R12 [FN]

CUANDO existan varias filas con la misma combinación `anio` y `semestre`, el
sistema DEBE conservar la primera ocurrencia y omitir las siguientes,
registrando las filas duplicadas.

## R13 [FN]

CUANDO existan filas válidas, el sistema DEBE devolver un `DataFrame` con
exactamente las columnas `anio` y `semestre`, ambas con tipo entero y sin
duplicados.

## R14 [No deseado]

SI ninguna fila válida permanece después de las omisiones, ENTONCES el sistema
DEBE devolver un `DataFrame` vacío y registrar el resultado como fallo de
calidad de datos.

## R15 [NF]

El sistema DEBE registrar mediante `logging` el archivo, hoja, número de fila,
valores originales y motivo de cada fila omitida, sin utilizar `print`.
