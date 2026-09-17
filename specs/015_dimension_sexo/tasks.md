# Tasks: 015 — Dimensión sexo

> Tasks atómicas y ordenadas. Cada `R<n>` está cubierta por al menos una task y
> cada requirement tiene tasks de prueba.

- [ ] T1 — Definir el contrato de entrada de la dimensión sexo (`codigo`,
      `descripcion`, tipos y nulabilidad). Cubre: R1.
- [ ] T2 — Implementar la normalización canónica de `codigo` (entero validado)
      y `descripcion` (strip + UPPER; acentos ignorados solo al comparar).
      Cubre: R3.
- [ ] T3 — Implementar la validación de contrato que aborta con error
      identificable cuando hay `codigo` nulo/no numérico, `descripcion` nula o
      `codigo` duplicado con distinta descripción. Cubre: R2.
- [ ] T4 — Garantizar la categoría `SIN INFORMACION` (código `9`) en el
      `DataFrame` resultante cuando el origen no la incluya. Cubre: R4.
- [ ] T5 — Corregir en la transformación SNIES el mapeo de `id_genero`
      nulo/0/inválido a `SIN INFORMACION` (9) en lugar de `TRANS` (3), con una
      función pura reutilizable. Cubre: R5.
- [ ] T6 — Retornar métricas de entrada, salida, afectados por el mapeo de
      `id_genero` y conflictos de contrato, de forma reproducible y sin `print`.
      Cubre: R7.
- [ ] T7 — Mantener la transformación sin efectos de persistencia (ninguna
      dependencia de `config.database` en los módulos nuevos). Cubre: R6.
- [ ] T8 — Documentar en el spec el esquema de `tb_dim_sexo`, la política
      insert-only con reporte y la resolución de `genero_id` en hechos.
      Cubre: R8.
- [ ] T9 — Test unitario del contrato de entrada y salida canónica.
      Cubre: R1, R3.
- [ ] T10 — Test unitario de la categoría `SIN INFORMACION` (código `9`).
      Cubre: R4, R6.
- [ ] T11 — Test de calidad para duplicados y nulos de la dimensión.
      Cubre: R2.
- [ ] T12 — Test de calidad para el mapeo de `id_genero` y la métrica de
      afectados. Cubre: R5, R7.

## Verificación de cierre

- [ ] `python3 -m compileall -q .`
- [ ] `ruff format --check .` y `ruff check .`
- [ ] `pytest tests/unit -q` y `pytest tests/data_quality -q`
- [ ] `bash init.sh` (integración/instrumentación no requerida en esta feature:
      `tests/integration` no aplica porque no hay persistencia).