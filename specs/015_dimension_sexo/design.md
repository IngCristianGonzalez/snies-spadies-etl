# Design: 015 — Dimensión sexo

## Resumen

Se formaliza la transformación de la dimensión sexo como **función pura que
devuelve un `DataFrame`** canónico (`codigo`, `descripcion`), corrigiendo la
regla heredada que mapeaba `id_genero` nulo/0 a `3` (TRANS). No se persiste en
esta feature: el esquema de `tb_dim_sexo`, la política de carga insert-only y la
resolución de `genero_id` en hechos se documentan para una feature posterior.

Grano afectado: una fila por categoría de sexo, identificada por `codigo`.

## Contratos

### Entrada

| Columna | Tipo lógico | Regla |
| --- | --- | --- |
| `codigo` | entero | Obligatorio, no nulo, único |
| `descripcion` | texto | Obligatorio, no nulo |

Origen: `data/dimensions/dim_sexo.xlsx` (válido actual: códigos 1–4, sin nulos,
sin duplicados).

### Salida

| Columna | Tipo lógico | Regla |
| --- | --- | --- |
| `codigo` | `int64` | Clave de negocio validada |
| `descripcion` | `str` | UPPER + strip; acentos ignorados solo al comparar |

Salida adicional garantizada: categoría `SIN INFORMACION` (código `9`) cuando el
origen no la incluya.

Efectos secundarios: ninguno persistente. La corrección de `id_genero` solo
transforma el `DataFrame` SNIES y devuelve una métrica de registros afectados.

Errores: `ValueError` (o excepción de contrato propia) con los códigos en
conflicto cuando se cumple R2.

## Archivos nuevos

- `etl/transform_sexo.py` — funciones puras: normalización de la dimensión y
  mapeo de códigos de género.
- `tests/unit/test_transform_sexo.py` — tests unitarios de R1, R3, R4, R6.
- `tests/data_quality/test_sexo_snies_quality.py` — tests de calidad R2, R5, R7.

## Archivos modificados

- `etl/transform.py` — sustituir en `mapear_columnas` y `transformar_snies` el
  reemplazo de `id_genero` nulo/0 a `3` por el mapeo a `9` mediante la función
  nueva; conservar el contrato actual de retorno (`DataFrame`).

## Decisiones técnicas

### ADR-001: Salida exclusivamente como `DataFrame`

**Contexto:** el entregable solicitado es una función que devuelve el
`DataFrame` normalizado; no se pidió cargar en PostgreSQL.

**Decisión:** la feature expone funciones puras que devuelven `pandas.DataFrame`.
Ningún módulo de esta feature importa `config.database` ni abre conexiones.

**Alternativa descartada:** incluir el loader idempotente `load_dim_sexos`
dentro del alcance.

**Consecuencias:** `tb_dim_sexo` no se modifica en esta feature; la persistencia
e idempotencia se planifican como feature separada con su spec.

### ADR-002: Categoría `SIN INFORMACION` (código `9`)

**Contexto:** la regla heredada reutilizaba la categoría `3` (TRANS) para
representar `id_genero` nulo/0, mezclando "trans" con "sin información" y
distorsionando el análisis.

**Decisión:** añadir la categoría `SIN INFORMACION` con código `9` como fila
garantizada de la dimensión y mapear `id_genero` nulo/0/inválido a `9`,
contabilizando los registros afectados.

**Alternativa descartada:** rechazar en cuarentena los registros con `id_genero`
inválido (el humano eligió conservar el grano y tipificar la ausencia).

**Consecuencias:** la dimensión pasa de 4 a 5 categorías; los hechos SNIES con
`id_genero` inválido se separan en la categoría explícita. Los registros ya
cargados con `genero_id = 3` por la regla vieja no se migran en esta feature
(fuera de alcance) y deben evaluarse antes de reprocesar periodos.

### ADR-003: Resolución de `genero_id` en hechos fuera de alcance

**Contexto:** `load_facts.py` toma `id_genero` directamente como `genero_id` sin
validarlo contra `tb_dim_sexo`.

**Decisión:** no se modifica `load_facts.py` en esta feature; la política de
resolución (validar `genero_id` contra la dimensión y reportar coincidencias y
faltantes) se documenta para la feature de persistencia.

**Alternativa descartada:** implementar el join contra `tb_dim_sexo` aquí.

**Consecuencias:** no cambia la carga de hechos; se evita alcance no aprobado.

### ADR-004: Esquema y políticas documentadas, no implementadas

**Contexto:** la tabla `tb_dim_sexo` existe en la base creada manualmente y no
está en `sql/ddl.sql`.

**Decisión:** el spec documenta el esquema esperado (`id` PK, `codigo` UNIQUE,
`descripcion`) y la política de carga insert-only con reporte de nuevas,
existentes y conflictivas, sin añadir DDL a `sql/ddl.sql` ni loader.

**Alternativa descartada:** versionar el DDL y crear el loader en esta feature.

**Consecuencias:** el esquema oficial queda descrito como contrato de destino
para la feature de persistencia.

## Riesgos y mitigaciones

- **Cambio analítico por la corrección de `id_genero`:** los valores que antes
  contaban como `TRANS` (nulo/0) ahora serán `SIN INFORMACION`. Se mitiga
  contabilizando los registros afectados por archivo y documentando el cambio
  en esta spec.
- **Datos históricos con `genero_id = 3`:** permanecen invariables hasta que se
  decida y apruebe un reprocesamiento; queda registrado fuera de alcance.
- **Catálogo futuro con código 9 distinto:** la función debe garantizar que la
  fila `9 / SIN INFORMACION` sea única; si el origen trae un código 9 con otra
  descripción, aplica el error de contrato de R2.

## Trazabilidad esperada `R ↔ Tests`

| Requirement | Test (propuesto) |
| --- | --- |
| R1 | `test_sexo_dimension_input_contract_accepted` |
| R2 | `test_sexo_duplicate_code_with_different_label_raises_error` |
| R3 | `test_sexo_dimension_returns_canonical_frame` |
| R4 | `test_sexo_dimension_includes_sin_informacion_code_9` |
| R5 | `test_snies_genero_null_zero_maps_to_sin_informacion` |
| R6 | `test_sexo_transform_has_no_db_dependency` |
| R7 | `test_snies_genero_mapping_counts_affected_rows` |
| R8 | cobertura documental (revisión de spec; sin test de código) |