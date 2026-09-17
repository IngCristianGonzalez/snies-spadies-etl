# Revisión — 015 Dimensión sexo

- **Revisado por:** Kerin Mindiola (kerinmindiola@gmail.com) — revisión directa
  del líder/implementer (el subagente `reviewer` no está disponible en este
  entorno).
- **Fecha:** 2026-09-17
- **Rama:** feature/015_dimension_sexo

## Alcance revisado

- `etl/transform_sexo.py` (nuevo) y `etl/transform.py` (modificado).
- `tests/unit/test_transform_sexo.py` y `tests/data_quality/test_sexo_snies_quality.py`.
- `progress/impl_015_dimension_sexo.md` (evidencia).
- Contra `specs/015_dimension_sexo/` y `docs/{conventions,data_quality,specs}.md`.

## Checklist

| Criterio | Resultado |
| --- | --- |
| Funciones puras, deterministas, sin `print` en módulos nuevos | OK |
| Sin dependencia de `config.database` / `create_engine` en lo nuevo | OK |
| Nulos e inválidos con política explícita y métrica (`map_genero_codes`) | OK |
| Duplicados: conflicto con descripción distinta aborta (R2); iguales se colapsan | OK |
| Error de contrato identifica códigos/registros (`SexoContractError`.conflicts) | OK |
| `SIN INFORMACION` (9) garantizada y prueba (R4) | OK |
| Correlación de la regla heredada: `id_genero` nulo/0/inválido → 9 (R5) | OK (smoke: [0,None,3,1,"x"] → [9,9,3,1,9]) |
| Salida solo DataFrame, sin persistencia (R6) | OK |
| Métricas reproducibles `METRIC_COLUMNS` (R7) | OK |
| Spec documenta esquema y políticas pendientes (R8) | OK |
| Tests verifican valores/métricas, no solo ausencia de excepción | OK |
| Trazabilidad R ↔ Tests en evidencia | OK |

## Hallazgos

### Mayores
Ninguno.

### Menores / deuda no introducida

- `etl/transform.py` conserva deuda global previa del repositorio (formato y
  `B006 codigos=["1120","1123"]`). No fue introducida por esta feature y no se
  refactoriza fuera de alcance.
- En la dimensión, los duplicados idénticos se colapsan con `drop_duplicates`;
  el conteo exacto queda a cargo de quien reporte con `sexo_dimension_metrics`
  (`duplicate_rows`) y no se calcula automáticamente dentro de la función.
- `mypy`/`pandas-stubs` y `openpyxl` no instalados en este entorno; no bloquean
  esta feature (sin persistencia ni lectura de Excel nuevo).

## Veredicto

Aprobada con observaciones menores. La feature puede cerrarse cuando el humano
autorice: commit semántico, actualización de `ENGINES.md`, `progress/history.md`
y estado en `feature_list.json`.