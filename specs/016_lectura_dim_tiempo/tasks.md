# Tasks: 016 — Lectura de dimensión tiempo

- [x] T1 — Definir la excepción y el contrato de entrada/salida del lector en
  `etl/extract_dimensions.py`. Cubre: R1, R3, R4, R5, R13.
- [x] T2 — Implementar la apertura del libro y el escaneo de hojas y filas de
  encabezado. Cubre: R1, R2, R4, R5.
- [x] T3 — Implementar la selección exclusiva de `anio` y `semestre` y la
  conversión de enteros válidos. Cubre: R6, R7, R13.
- [x] T4 — Implementar la validación de año positivo y semestre 1/2 con
  clasificación de filas omitidas. Cubre: R8, R9, R10, R11.
- [x] T5 — Implementar la deduplicación estable por `(anio, semestre)` y el
  reporte de duplicados. Cubre: R12, R13.
- [x] T6 — Implementar logging estructurado para filas omitidas y resultado
  vacío. Cubre: R10, R11, R12, R14, R15.
- [ ] T7 — Integrar el lector en `main_dimensiones.py` sin añadir carga a
  PostgreSQL. Cubre: R1, R6, R13.
- [ ] T8 — Testear archivo inexistente con error descriptivo. Cubre: R1, R3.
- [ ] T9 — Testear detección de encabezado después de filas introductorias y
  columnas adicionales. Cubre: R2, R6.
- [ ] T10 — Testear ausencia de columnas y múltiples hojas candidatas. Cubre:
  R4, R5.
- [ ] T11 — Testear conversión de enteros y rechazo de valores no enteros,
  años no positivos y semestres inválidos. Cubre: R7, R8, R9, R10.
- [ ] T12 — Testear omisión de filas vacías y verificar sus eventos de logging.
  Cubre: R11, R15.
- [ ] T13 — Testear conservación de la primera ocurrencia y logging de
  duplicados. Cubre: R12, R15.
- [ ] T14 — Testear `DataFrame` vacío y evento de error cuando no quedan filas
  válidas. Cubre: R13, R14.
- [ ] T15 — Ejecutar compileall, Ruff, mypy y pytest aplicables y documentar
  evidencia R↔Tests. Cubre: R1-R15.
