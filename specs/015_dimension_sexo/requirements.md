# Requirements: 015 — Dimensión sexo

> Formato EARS. Cada requirement tiene un ID estable, un solo `DEBE` y un
> resultado verificable. [FN] funcional, [NF] no funcional.

## R1 [FN] — Contrato de entrada

El sistema DEBE aceptar para la dimensión sexo un `DataFrame` de entrada con las
columnas `codigo` y `descripcion`.

- `codigo`: representación numérica de la clave de negocio del sexo.
- `descripcion`: texto legible de la categoría.

## R2 [No deseado] — Errores de contrato del catálogo

SI la entrada presenta un `codigo` nulo o no numérico, una `descripcion` nula o
un `codigo` duplicado con `descripcion` distinta, ENTONCES el sistema DEBE
abortar la transformación con un error de contrato que identifique los códigos
o registros en conflicto y no debe devolver un `DataFrame`.

## R3 [FN] — Salida canónica

El sistema DEBE devolver un `DataFrame` canónico con las columnas `codigo`
(entero) y `descripcion` (texto) donde la descripción está normalizada con
espacios externos eliminados y convertida a mayúsculas, ignorando acentos solo
para efectos de comparación y conservando el texto de exhibición.

## R4 [FN] — Categoría SIN INFORMACION

El sistema DEBE asegurar que el `DataFrame` resultante incluya la categoría
`SIN INFORMACION` con `codigo` igual a `9` cuando el catálogo de origen no la
contenga.

## R5 [FN] — Corrección de la regla heredada de id_genero

CUANDO un registro SNIES presente `id_genero` nulo, igual a `0` o inválido,
ENTONCES el sistema DEBE mapearlo a la categoría `SIN INFORMACION` (código `9`)
y contabilizar los registros afectados, de modo que ya no se reutilice la
categoría `TRANS` (código `3`) para ese propósito.

## R6 [NF] — Sin efectos de persistencia

La transformación de la dimensión DEBE ejecutarse sin escribir en PostgreSQL ni
en otro destino persistente y DEBE retornar el `DataFrame` resultante.

## R7 [NF] — Métricas de transformación

El sistema DEBE reportar para la transformación las filas de entrada, las filas
de salida, los registros afectados por el mapeo de `id_genero` y los conflictos
de contrato, de forma reproducible y no dependiente de `print`.

## R8 [NF] — Documentación de persistencia pendiente

El spec DEBE documentar, sin implementarlas, el esquema de `tb_dim_sexo`, la
política de carga idempotente (insert-only con reporte de nuevas, existentes y
conflictivas) y la política de resolución de `genero_id` en los hechos SNIES,
que quedan para una feature de persistencia.