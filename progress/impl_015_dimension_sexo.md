# Evidencia de implementación — 015 Dimensión sexo

- **Rama:** feature/015_dimension_sexo
- **Ejecutado por:** Kerin Mindiola (kerinmindiola@gmail.com)
- **Fecha:** 2026-09-17
- **Spec aprobado:** 2026-09-17 (humano)

## Resumen

Se implementa la transformación pura de la dimensión sexo (salida `DataFrame`
canónico `[codigo, descripcion]`) y la corrección de la regla heredada que
mapeaba `id_genero` nulo/0 a `3` (TRANS). Persistencia, DDL y resolución en
hechos quedan documentadas y fuera de alcance por decisión del spec.

## Archivos

- Nuevo `etl/transform_sexo.py` — normalize_sexo_dimension, validate_sexo_contract,
  map_genero_codes, sexo_dimension_metrics, SexoContractError, constantes.
- Modificado `etl/transform.py` — mapear_columnas usa `map_genero_codes`
  (nulo/0/inválido → SIN INFORMACION 9); se eliminó el import de pandas que
  quedó sin uso.
- Nuevo `tests/unit/test_transform_sexo.py` — R1, R3, R4, R6.
- Nuevo `tests/data_quality/test_sexo_snies_quality.py` — R2, R5, R7.

## Trazabilidad R ↔ Tests

| Requirement | Test | Resultado |
| --- | --- | --- |
| R1 | `test_sexo_dimension_input_contract_accepted` | PASS |
| R1/R2 | `test_sexo_missing_columns_raises_error` | PASS |
| R2 | `test_sexo_duplicate_code_with_different_label_raises_error` | PASS |
| R3 | `test_sexo_dimension_returns_canonical_frame` | PASS |
| R4 | `test_sexo_dimension_includes_sin_informacion_code_9` | PASS |
| R5 | `test_snies_genero_null_zero_maps_to_sin_informacion` | PASS |
| R7 | `test_snies_genero_mapping_counts_affected_rows` | PASS |
| R6 | `test_sexo_transform_has_no_db_dependency` | PASS |
| R7 | `test_sexo_metrics_report_expected_columns` | PASS |
| R8 | Revisión editorial de `specs/015_dimension_sexo/*.md` (sin test de código) | PASS |

Tests adicionales de comportamiento: `test_sexo_duplicate_code_same_label_is_collapsed`,
`test_sexo_null_or_non_numeric_code_raises_error`, `test_sexo_null_description_raises_error`.

## Evidencia de comandos

| Comando | Resultado |
| --- | --- |
| `python -m compileall -q etl tests config main.py main_dimensiones.py main_spadies.py` | OK |
| `python -m pytest tests/unit tests/data_quality -q` | 12 passed |
| `python -m ruff check etl/transform_sexo.py tests/...` (archivos nuevos) | All checks passed |
| `python -m ruff format --check` (archivos nuevos) | Already formatted |
| `git diff --check` | OK |
| Smoke `mapear_columnas` con `id_genero` [0, None, 3, 1, "x"] | → [9, 9, 3, 1, 9] |

## Deuda pre-existente no introducida por esta feature

- `bash init.sh` no ejecuta `--full` directamente en Windows (CRLF): se validó
  vía WSL. `kickstart.json` local con runtime `python3`.
- `ruff check .` reporta deuda global previa; en `etl/transform.py` persiste
  `B006 codigos=["1120","1123"]` (no introducido por esta feature).
- `tests/integration` no aplica: no hay persistencia en el alcance.
- `mypy` y `pandas-stubs` no instalados en este entorno; el tipado se declara en
  los módulos nuevos aunque mypy no se ejecutó.
- Herramientas `pytest` 8.4.2 y `ruff` 0.16.8 instaladas en el Python de Windows
  (documentado en requirements-dev.txt).