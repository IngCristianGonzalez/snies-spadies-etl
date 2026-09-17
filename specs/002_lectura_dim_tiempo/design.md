# Design: 002 — Lectura de dimensión tiempo

## Resumen

Se implementará un lector aislado para `data/dimensions/dim_tiempo.xlsx`. El
lector inspeccionará todas las hojas, detectará una única fila de encabezados
con `anio` y `semestre`, validará las filas en orden, conservará la primera
ocurrencia de cada periodo y devolverá únicamente el `DataFrame` canónico.

La feature no cargará PostgreSQL ni modificará la lógica de persistencia.

## Contrato de entrada

- Ruta recibida explícitamente como `Path`; la interfaz actual pasará
  `data/dimensions/dim_tiempo.xlsx`.
- Libro Excel legible por `openpyxl`.
- Una y solo una hoja debe contener encabezados `anio` y `semestre`.
- Las columnas pueden aparecer junto con columnas adicionales.
- Los encabezados se comparan después de eliminar espacios externos y aplicar
  comparación sin distinguir mayúsculas/minúsculas.
- Las filas anteriores al encabezado se consideran introductorias y no se
  transforman en datos.

## Contrato de salida

```text
DataFrame con columnas, en este orden:
    anio: int
    semestre: int
```

La salida no contiene columnas auxiliares, filas inválidas ni duplicados. El
orden de las filas válidas conserva el orden de aparición en la hoja.

## Contrato de errores y logging

### Errores estructurales

Se lanzará una excepción específica de lectura para:

- Archivo inexistente o ilegible.
- Ninguna hoja candidata.
- Más de una hoja candidata.

El mensaje incluirá ruta, hojas revisadas, hojas candidatas cuando aplique y
las columnas observadas.

### Filas omitidas

Las filas completamente vacías, no convertibles, con año no positivo,
semestre distinto de `1`/`2` y duplicadas no detienen el proceso. Se registran
con `logging` a nivel `WARNING` con:

- Evento estable.
- Ruta y nombre de hoja.
- Número de fila original, comenzando en uno.
- Valores originales de `anio` y `semestre`.
- Regla incumplida.
- Acción tomada.

Si ninguna fila válida permanece, la función devuelve un `DataFrame` vacío y
registra un evento `ERROR` de calidad de datos. No se lanza una excepción por
ese caso.

## Archivos nuevos y modificados

### Nuevos

- `etl/extract_dimensions.py` — lector y validación de `dim_tiempo`.
- `etl/exceptions.py` — excepción específica para errores estructurales de
  lectura, si el contrato existente no ofrece una equivalente.
- `tests/unit/test_extract_dimensions.py` — pruebas unitarias con libros
  temporales.

### Modificados

- `main_dimensiones.py` — invocar el lector sin duplicar la lógica de lectura.
- `progress/impl_lectura_dim_tiempo.md` — registrar trazabilidad y evidencia.

No se modifican `sql/ddl.sql`, loaders ni tablas PostgreSQL en esta feature.

## Flujo técnico

1. Recibir y validar la ruta.
2. Abrir el libro con `pandas.ExcelFile` y `openpyxl`.
3. Leer cada hoja sin asumir nombre ni posición del encabezado.
4. Normalizar temporalmente los valores de las filas para detectar los
   encabezados requeridos.
5. Fallar si hay cero o más de una hoja candidata.
6. Leer la hoja candidata usando la fila de encabezado detectada.
7. Seleccionar solo `anio` y `semestre`.
8. Recorrer las filas conservando el número de fila de origen.
9. Convertir representaciones enteras textuales y validar año positivo y
   semestre 1/2.
10. Omitir y registrar filas inválidas o vacías.
11. Conservar la primera fila de cada clave `(anio, semestre)` y registrar las
    repeticiones.
12. Devolver el `DataFrame` canónico y sus tipos enteros.

## ADRs

### ADR-001: Escanear hojas y encabezados antes de leer el esquema

**Contexto:** El nombre de la hoja no es un contrato confiable y puede haber
filas introductorias antes del encabezado.

**Decisión:** Inspeccionar todas las hojas sin encabezado y seleccionar una
única hoja que contenga ambos nombres de columna normalizados.

**Alternativa descartada:** Leer siempre `Hoja1` con `header=0`, porque falla
cuando cambia el nombre de la hoja o existen filas introductorias.

**Consecuencias:** El lector requiere una primera pasada por el libro, pero
produce diagnósticos claros y evita seleccionar una hoja incorrecta.

### ADR-002: Omitir filas inválidas y conservar la primera duplicada

**Contexto:** El contrato solicita continuar con filas válidas sin perder la
trazabilidad de los problemas.

**Decisión:** Validar fila a fila, registrar cada omisión y conservar la
primera aparición de cada `(anio, semestre)`.

**Alternativa descartada:** Fallar todo el archivo ante el primer valor inválido
o usar `drop_duplicates` sin identificar filas de origen.

**Consecuencias:** La salida puede ser parcial; el logging y sus métricas son
obligatorios para que esa parcialidad sea visible.

### ADR-003: Mantener la lectura separada de la carga

**Contexto:** Esta feature solo debe producir un `DataFrame` verificable.

**Decisión:** No abrir conexiones ni llamar loaders desde el lector.

**Alternativa descartada:** Reutilizar `main_dimensiones.py` para leer y cargar
en el mismo flujo, porque mezcla extracción con persistencia y dificulta las
pruebas.

**Consecuencias:** La integración con PostgreSQL se realizará en una feature
posterior.

## Riesgos y mitigaciones

| Riesgo | Mitigación |
| --- | --- |
| Dos hojas parecen válidas | Fallar y listar candidatas; no elegir arbitrariamente |
| Encabezados con mayúsculas o espacios | Normalización solo para comparación |
| Pérdida de filas inválidas | Logging con fila, valores y regla |
| Duplicados silenciosos | Conservar primera ocurrencia y registrar repetidas |
| Diferencias entre pandas y Excel | Tests con libros temporales reales |

## Trazabilidad preliminar

| Requirement | Diseño |
| --- | --- |
| R1-R5 | Flujo técnico 1-6 y ADR-001 |
| R6-R7 | Flujo técnico 7-9 |
| R8-R12 | Flujo técnico 9-11 y ADR-002 |
| R13-R14 | Flujo técnico 11-12 |
| R15 | Contrato de errores y logging |
