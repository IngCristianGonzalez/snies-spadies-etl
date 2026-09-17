# Verificación — ETL Python SNIES/SPADIES

> El agente no afirma que el pipeline funciona. Lo demuestra con comandos,
> pruebas reproducibles, métricas de calidad y evidencia registrada.

## 1. Puertas de verificación

Una entrega de código avanza por estas puertas:

0. Entorno y configuración.
1. Sintaxis, formato y lint.
2. Tipado estático.
3. Tests unitarios puros.
4. Tests de calidad de datos.
5. Tests de integración con PostgreSQL.
6. Tests de idempotencia y control de lotes.
7. Smoke test del pipeline.
8. Seguridad y reproducibilidad.
9. Trazabilidad y revisión humana.

Si una puerta obligatoria falla, no se debe marcar la feature como completada.
Las puertas que no apliquen deben indicar la razón en la evidencia.

## 2. Nivel 0 — Entorno

Comandos base:

```bash
bash init.sh
python3 --version
python3 -m pip check
```

El entorno debe confirmar:

- Python 3.11 o la versión mínima declarada.
- Dependencias instaladas de forma reproducible.
- Variables de entorno requeridas identificadas sin mostrar secretos.
- PostgreSQL disponible para pruebas de integración cuando correspondan.
- No existen rutas de entrada o salida personales codificadas en el código.

`bash init.sh --full` debe ejecutar los pasos declarados en `kickstart.json`.
La configuración del harness debe apuntar a comandos Python reales y a rutas que
existan en este repositorio.

## 3. Nivel 1 — Sintaxis, formato y lint

Comandos objetivo:

```bash
python3 -m compileall -q .
ruff format --check .
ruff check .
```

No se aceptan:

- Errores de sintaxis.
- Imports no usados o imposibles.
- Variables definidas y no utilizadas.
- Excepciones capturadas sin tratamiento.
- Código de depuración sin propósito.
- Uso de `print` en módulos de producción cuando exista logging.
- Líneas o reglas deshabilitadas globalmente para ocultar problemas.

Las excepciones de lint deben ser locales, justificadas y revisadas. No se debe
deshabilitar una familia completa de reglas para hacer pasar la verificación.

## 4. Nivel 2 — Tipado estático

Comando objetivo:

```bash
mypy src etl config
```

Si el proyecto adopta Pyright en vez de mypy, el comando debe quedar declarado
en `kickstart.json` y aplicarse consistentemente.

El tipado debe cubrir especialmente:

- Configuración.
- Puertos y adaptadores.
- Repositorios y loaders.
- Servicios de aplicación.
- Funciones públicas de transformación.
- Resultados de validación y reportes de ejecución.

La ausencia de un tipo completo para un `DataFrame` no autoriza a usar `Any` en
toda la cadena. El contrato de columnas debe estar documentado y validado en
runtime.

## 5. Nivel 3 — Tests unitarios

Comando objetivo:

```bash
pytest tests/unit -q
```

Los tests unitarios deben ser rápidos, deterministas y no requerir:

- PostgreSQL.
- Archivos de datos personales.
- APIs externas.
- Variables de entorno secretas.
- Orden de ejecución compartido.

Deben cubrir como mínimo:

- Normalización de nombres de columnas.
- Detección de columnas de medición.
- Mapeo de columnas SNIES.
- Transformación de periodos SPADIES.
- Conversión de tipos.
- Filtros de negocio.
- Reglas de nulos y rangos.
- Detección de duplicados.
- Clasificación de errores y rechazos.

Ejemplo de estructura:

```python
def test_transform_snies_with_valid_frame_returns_canonical_columns():
    source_frame = ...

    result = transform_snies(source_frame, "matriculados")

    assert list(result.columns) == EXPECTED_SNIES_COLUMNS
    assert result["tipo"].eq("matriculados").all()
```

Cada test debe seguir Arrange, Act, Assert y verificar un resultado concreto,
no solo que la función termina sin lanzar una excepción.

## 6. Nivel 4 — Tests de calidad de datos

Comando objetivo:

```bash
pytest tests/unit tests/data_quality -q
```

Cada requisito de `docs/data_quality.md` debe tener datos de prueba que cubran:

- Entrada válida.
- Columna requerida ausente.
- Encabezado no detectado.
- Código con ceros significativos.
- Nulo obligatorio.
- Valor inválido convertido por error.
- Periodo imposible.
- Semestre fuera del dominio.
- Porcentaje fuera de rango.
- Duplicado en el grano.
- Clave de dimensión no encontrada.
- Unión con cardinalidad incorrecta.
- Rechazo con contexto suficiente.

Los tests deben verificar también `input_rows`, `output_rows`, `rejected_rows`,
`unmatched_rows` y las demás métricas que el caso de uso declare.

## 7. Nivel 5 — Tests de integración

Comando objetivo:

```bash
pytest tests/integration -q
```

Estos tests verifican la interacción real con PostgreSQL y SQLAlchemy:

- Conexión usando configuración de prueba.
- Creación o migración del esquema requerido.
- Inserción y lectura de dimensiones.
- Carga de hechos.
- Restricciones de unicidad y claves foráneas.
- Transacciones y rollback.
- Consultas parametrizadas.
- Registro del control de lotes.

Se prefiere un contenedor PostgreSQL desechable o una base aislada de pruebas.
Nunca se ejecutan tests contra una base de producción.

Los tests de integración deben preparar sus datos y limpiar el estado sin
depender del orden de ejecución. Si una prueba requiere Docker, debe indicarlo
en la documentación y en la evidencia.

## 8. Nivel 6 — Idempotencia y control de lotes

Cada pipeline de carga debe tener pruebas para:

1. Ejecutar un lote nuevo y comprobar la carga esperada.
2. Ejecutar el mismo lote otra vez.
3. Verificar que no duplica hechos ni dimensiones.
4. Confirmar la política elegida: ignorar, actualizar o reemplazar.
5. Simular un fallo antes de registrar el lote como completado.
6. Reintentar un lote fallido y comprobar el resultado.

La prueba debe revisar tanto las filas de destino como la tabla de control.
Un proceso que deja datos cargados pero registra éxito cuando una etapa falló
no es idempotente ni verificable.

## 9. Nivel 7 — Smoke test del pipeline

El smoke test usa fixtures pequeñas, controladas y versionadas:

```bash
pytest tests/smoke -q
```

Debe comprobar el flujo principal de cada fuente:

- SNIES: lectura, normalización, validación y preparación de carga.
- SPADIES: lectura, unpivot, periodo, validación y preparación de carga.
- Dimensiones: carga de catálogo y resolución de claves.

El smoke test no debe depender de los archivos completos del directorio `data/`
ni de datos que cambien sin control. Su propósito es detectar roturas del flujo,
no medir volumen.

## 10. Nivel 8 — Seguridad y reproducibilidad

Comandos y controles objetivo:

```bash
python3 -m pip check
ruff check .
git diff --check
```

Además se verifica manual o automáticamente que:

- No hay contraseñas, tokens o URLs privadas en el código.
- `.env` y archivos temporales están excluidos del control de versiones.
- Las consultas usan parámetros.
- Los logs no contienen secretos ni filas completas innecesarias.
- Las dependencias tienen versiones compatibles y sin vulnerabilidades
  conocidas según la herramienta adoptada.
- Un fixture produce el mismo resultado en ejecuciones repetidas.
- Las zonas horarias, codificaciones y separadores están declarados.

## 11. Nivel 9 — Trazabilidad

Cada requirement de una feature debe mapearse a uno o más tests y a evidencia
de ejecución:

```markdown
| Requirement | Test | Evidencia |
| --- | --- | --- |
| R1 | `test_transform_valid_frame_returns_canonical_columns` | pytest verde |
| R2 | `test_missing_required_column_returns_rejection` | pytest verde |
```

El implementador registra el mapa en `progress/impl_<feature>.md`. El revisor
comprueba que los nombres existen, los tests pasan y no hay requirements sin
cobertura.

## 12. Cobertura

La cobertura se usa para localizar código no probado, no como único indicador
de calidad. Comando objetivo:

```bash
pytest --cov=src --cov=etl --cov=config --cov-report=term-missing
```

Como objetivo inicial, las transformaciones puras y reglas de calidad deben
alcanzar al menos 90% de líneas. Los adaptadores de infraestructura se evalúan
con tests de integración y no se deben excluir automáticamente para mejorar el
porcentaje.

Una cobertura alta no compensa tests que no verifican valores, métricas,
cardinalidad o errores.

## 13. Rendimiento

No se optimiza antes de medir. Cuando el volumen lo justifique, se deben medir:

- Tiempo de extracción.
- Tiempo de transformación.
- Tiempo y memoria de los `merge`.
- Tiempo de resolución de dimensiones.
- Tiempo de carga.
- Tamaño de lotes y filas por segundo.

Las optimizaciones deben conservar el resultado, el grano y las métricas de
calidad. Los cambios de rendimiento requieren un benchmark reproducible o una
comparación documentada con un fixture representativo.

## 14. Evidencia de cierre

Una feature solo puede pasar a `done` con un reporte que incluya:

- Commit o rama verificada.
- Comandos ejecutados.
- Resultado y duración relevante.
- Tests ejecutados y cantidad de tests.
- Cobertura, si aplica.
- Validaciones de calidad de datos.
- Tests de integración, si aplica.
- Vulnerabilidades o limitaciones conocidas.
- Mapa de trazabilidad R ↔ Tests.
- Incidencias pendientes fuera del alcance.

El reporte se registra en `progress/review_<feature>.md` y no reemplaza los
logs detallados de una ejecución del pipeline.

## 15. Criterio de aceptación

Este documento se considera aprobado cuando define comandos ejecutables para el
stack Python, separa tests unitarios de integración, exige verificación de
calidad e idempotencia, y establece la evidencia necesaria antes de cerrar una
feature.
