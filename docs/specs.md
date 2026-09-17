# Spec Driven Development (SDD)

Este repositorio usa SDD para convertir una necesidad de datos en una
implementación verificable. El orden obligatorio es:

```text
requirements.md → design.md → tasks.md → código y tests → revisión
```

El spec es el contrato. Si una regla de negocio, un cambio de grano, un filtro
o una política de carga no está aprobada en el spec, no se implementa.

## 1. Principios

1. Escribir el qué antes del código.
2. Documentar las decisiones técnicas antes de implementarlas.
3. Mantener una feature por spec.
4. Hacer que cada requirement sea verificable.
5. Esperar aprobación humana antes de escribir código de una feature SDD.
6. Mantener trazabilidad entre requirements, tasks, tests y evidencia.
7. Volver a aprobación si aparece una ambigüedad de datos o persistencia.

## 2. Estructura

Una feature con `"sdd": true` usa:

```text
specs/
└── {NNN}_{snake_case_name}/
    ├── requirements.md
    ├── design.md
    └── tasks.md
```

La rama correspondiente es `feature/{NNN}_{snake_case_name}`. El identificador
debe coincidir con `feature_list.json`.

## 3. Estados

```text
pending → spec_ready → in_progress → done
              │              │
              │              └→ blocked
              └→ pending si el humano solicita cambios
```

| Estado | Significado |
| --- | --- |
| `pending` | No existe un spec aprobado |
| `spec_ready` | Los tres documentos están listos para revisión humana |
| `in_progress` | El spec fue aprobado y se implementa |
| `blocked` | Existe una dependencia o decisión pendiente |
| `done` | Código, tests, revisión y evidencia están completos |

La aprobación de documentación de contexto no aprueba automáticamente código
de producción.

## 4. `requirements.md` — EARS

Cada requirement tiene un identificador estable y un solo resultado obligatorio.
Se usan estos patrones:

| Tipo | Plantilla |
| --- | --- |
| Ubicuo | `El sistema DEBE <acción>.` |
| Evento | `CUANDO <disparador>, el sistema DEBE <acción>.` |
| Estado | `MIENTRAS <estado>, el sistema DEBE <acción>.` |
| Opcional | `DONDE <condición>, el sistema DEBE <acción>.` |
| No deseado | `SI <evento>, ENTONCES el sistema DEBE <acción>.` |

Los requisitos funcionales pueden usar `[FN]`, los no funcionales `[NF]`, los
de seguridad `[SEC]` y los de rendimiento `[PERF]`.

### Reglas duras

- IDs secuenciales: `R1`, `R2`, ...
- Un solo `DEBE` por requirement.
- Sin verbos blandos como "podría", "sería deseable" o "correctamente" sin
  una métrica.
- No describir una implementación concreta en los requirements.
- Cada requirement debe tener al menos un test posible.
- Cada requirement debe declarar el dato, grano o resultado afectado cuando
  corresponda.

### Ejemplo ETL

```markdown
## R1 [FN]

El sistema DEBE rechazar un archivo SNIES cuando falte una columna obligatoria
del esquema canónico.

## R2 [FN]

CUANDO un archivo SPADIES tenga periodos válidos, el sistema DEBE convertirlos
a filas con variable, categoría, año, semestre y porcentaje.

## R3 [NF]

El sistema DEBE reportar filas de entrada, salida, rechazadas y claves no
resueltas para cada etapa crítica.

## R4 [No deseado]

SI una clave de dimensión no puede resolverse, ENTONCES el sistema DEBE aplicar
la política aprobada y registrar las filas afectadas.
```

## 5. `design.md` — diseño y ADRs

El diseño debe explicar cómo se implementa el spec sin convertirse en código.
Debe incluir:

```markdown
# Design: {NNN} — {nombre}

## Resumen
Qué se cambia, por qué y cuál es el grano afectado.

## Contratos
Entradas, salidas, columnas, tipos y efectos secundarios.

## Archivos nuevos
- `etl/...` — responsabilidad.

## Archivos modificados
- `tests/...` — cobertura.

## Decisiones técnicas
### ADR-001: título
**Contexto:** ...
**Decisión:** ...
**Alternativa descartada:** ...
**Consecuencias:** ...

## Riesgos y mitigaciones
...
```

### ADRs para este ETL

Las decisiones importantes deben justificar, según aplique:

- Función estructurada frente a clase.
- Contrato de `DataFrame` y política de copia/mutación.
- Estrategia de `merge` y cardinalidad.
- Política de nulos y registros rechazados.
- Resolución de dimensiones.
- Transacción e idempotencia de la carga.
- Nueva dependencia de Python o PostgreSQL.

Cada ADR incluye contexto, decisión, alternativa descartada y consecuencias.
Si contradice `docs/architecture.md` o `docs/data_quality.md`, debe explicarlo
y requiere aprobación humana.

## 6. `tasks.md` — tareas ejecutables

Una task es atómica, está ordenada y referencia al menos un requirement:

```markdown
# Tasks: {NNN} — {nombre}

- [ ] T1 — Definir el contrato de entrada SNIES. Cubre: R1.
- [ ] T2 — Implementar la transformación canónica. Cubre: R1, R2.
- [ ] T3 — Añadir validación de grano y duplicados. Cubre: R3.
- [ ] T4 — Test unitario para columna obligatoria ausente. Cubre: R1.
- [ ] T5 — Test de calidad para periodo SPADIES inválido. Cubre: R2.
- [ ] T6 — Test de integración para reejecución del lote. Cubre: R3, R4.
```

Reglas:

- IDs secuenciales `T1`, `T2`, ...
- Verbos de acción: definir, crear, implementar, validar, probar, documentar.
- Cada `R<n>` aparece en al menos una task.
- Cada requirement tiene tasks de prueba.
- No mezclar extracción, transformación y carga en una task vaga.
- Máximo recomendado: 20 tasks por feature.
- No incluir acciones fuera del alcance aprobado.

## 7. Trazabilidad `R ↔ Tests`

El implementador registra el mapa en `progress/impl_<feature>.md`:

```markdown
| Requirement | Test | Resultado |
| --- | --- | --- |
| R1 | `test_missing_required_column_is_rejected` | PASS |
| R2 | `test_spadies_period_is_unpivoted` | PASS |
| R3 | `test_pipeline_reports_stage_metrics` | PASS |
| R4 | `test_unresolved_dimension_uses_policy` | PASS |
```

Un test debe demostrar el comportamiento, no solo que no se lanzó una
excepción. En calidad de datos debe comprobar también métricas y motivos de
rechazo.

## 8. Definition of Ready

Una feature está lista para implementar cuando:

- `requirements.md` usa EARS y tiene IDs únicos.
- Cada requirement es claro y verificable.
- `design.md` declara contratos, archivos, decisiones y riesgos.
- Cada ADR importante tiene una alternativa descartada.
- `tasks.md` cubre todos los requirements.
- Hay tasks de tests unitarios y de integración cuando aplican.
- Los tres archivos son consistentes.
- El humano aprobó el spec.
- No existe conflicto con otra feature activa.

## 9. Definition of Done

Una feature queda `done` solo cuando:

- Todas las tasks están marcadas `[x]`.
- Se preservan grano y semántica del dato.
- Pasan compileall, Ruff, tipado y tests aplicables.
- Pasan los controles de calidad de datos.
- Se probaron errores y reejecución cuando aplica.
- Existe trazabilidad completa `R ↔ Tests`.
- El reviewer aprobó la implementación.
- `feature_list.json` refleja el estado real.
- `progress/history.md` contiene la evidencia y el ejecutor.
- `ENGINES.md` se actualiza en la rama de feature.

## 10. Cambios durante implementación

El implementador debe detenerse y volver a `spec_ready` si descubre:

- Ambigüedad en el significado de una columna.
- Cambio requerido en el grano.
- Nueva política de filtro, nulos o rechazos.
- Cambio de esquema o de idempotencia.
- Decisión técnica incompatible con el design aprobado.

Un cambio puramente interno puede añadirse como task y documentarse si no
modifica el contrato. Todo cambio de datos o alcance necesita aprobación.

## 11. Checklist de revisión humana

### Requirements

- [ ] Todos los IDs son únicos y secuenciales.
- [ ] Todos usan EARS.
- [ ] Cada uno tiene un solo resultado obligatorio.
- [ ] Cada uno es verificable y no ambiguo.
- [ ] El grano y las reglas de calidad están claros.

### Design

- [ ] Entradas, salidas y efectos secundarios están declarados.
- [ ] Las decisiones de arquitectura tienen ADR.
- [ ] Se consideró función estructurada frente a POO.
- [ ] Las uniones declaran cardinalidad y política de faltantes.
- [ ] Los riesgos y migración están documentados.

### Tasks y trazabilidad

- [ ] Cada task cubre un requirement.
- [ ] Cada requirement tiene una task de test.
- [ ] Las tasks son atómicas y están ordenadas.
- [ ] Existe un plan de métricas y evidencia.

## 12. Anti-patrones

- Escribir código antes de aprobar requirements y design.
- Describir una librería concreta en vez del comportamiento esperado.
- Cambiar el grano en una task de refactorización.
- Usar `drop_duplicates` como sustituto de una decisión de negocio.
- Omitir tests de registros rechazados o claves no resueltas.
- Marcar una carga como exitosa sin conciliación.
- Añadir una clase solo para envolver una función pura.
- Cambiar el spec durante implementación sin volver a aprobación.
