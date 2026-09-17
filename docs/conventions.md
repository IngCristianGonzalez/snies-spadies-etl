# Convenciones de código — Python ETL

> Estas convenciones hacen que el código sea legible, predecible y fácil de
> verificar. Se aplican a módulos nuevos y a cualquier módulo que se modifique.

## 1. Versión y herramientas

- Python mínimo: 3.11, salvo decisión documentada.
- Formato y lint: Ruff.
- Tests: pytest.
- Tipado: type hints en funciones públicas y componentes de aplicación.
- Acceso a PostgreSQL: SQLAlchemy con consultas parametrizadas.
- Datos tabulares: pandas con contratos de columnas explícitos.
- Configuración local: variables de entorno cargadas desde `.env` solo fuera
  del código de producción.

Las versiones nuevas de dependencias deben fijarse o acotarse de forma
reproducible y revisarse antes de incorporarse.

## 2. Estructura de módulos

Cada módulo debe tener una única responsabilidad principal. La estructura
objetivo es:

```text
src/
├── domain/
├── application/
├── infrastructure/
└── interfaces/
tests/
├── unit/
├── integration/
└── fixtures/
```

Mientras se migra el código existente, los módulos bajo `etl/` deben seguir
estas reglas:

- `etl/extract.py`: lectura y extracción, sin reglas de carga.
- `etl/transform.py`: transformaciones puras y normalización.
- `etl/validate/`: reglas de calidad y reportes de validación.
- `etl/loads/`: persistencia y operaciones de carga.
- `etl/control.py`: control de lotes y estado de ejecución.
- `config/`: configuración y creación de dependencias, sin lógica de negocio.
- `main*.py`: composición y entrada, no transformación interna.

Un archivo debe contener una sola responsabilidad cohesiva. Si un módulo supera
aproximadamente 300 líneas o mezcla dos capas, debe considerarse candidato a
refactorización.

## 3. Nombres

| Elemento | Convención | Ejemplo |
| --- | --- | --- |
| Módulo | `snake_case` | `snies_pipeline.py` |
| Función | verbo en `snake_case` | `normalize_columns` |
| Variable | sustantivo en `snake_case` | `source_frame` |
| Clase | `PascalCase` | `PostgresFactLoader` |
| Protocolo | sustantivo o capacidad | `TabularReader` |
| Constante | `UPPER_SNAKE_CASE` | `SUPPORTED_EXTENSIONS` |
| Test | `test_<acción>_<escenario>_<resultado>` | `test_transform_invalid_period_raises_error` |
| Fixture | nombre descriptivo | `snies_raw_frame` |

Reglas adicionales:

- Usar nombres completos; evitar abreviaturas no estandarizadas.
- `df` solo se permite en funciones muy cortas y cuando el tipo de datos sea
  inequívoco. Preferir `source_frame`, `normalized_frame` o `fact_frame`.
- Los booleanos deben comenzar por `is_`, `has_`, `can_` o `should_`.
- Las funciones que hacen I/O deben expresar su acción: `read_excel_file`,
  `load_fact_rows`, `register_batch`.
- No usar nombres genéricos como `process`, `data`, `result` o `helper` si no
  describen la responsabilidad.

## 4. Tipado

Las funciones públicas deben declarar tipos de parámetros y retorno. Se prefiere
la sintaxis moderna de Python 3.11:

```python
from pathlib import Path

import pandas as pd


def read_csv_file(path: Path, *, separator: str = ";") -> pd.DataFrame:
    """Read a CSV source into a raw DataFrame."""
    return pd.read_csv(path, sep=separator)
```

Reglas:

- No usar `Any` como escape por defecto.
- Si el tipo de un `DataFrame` no puede expresarse completamente, documentar
  sus columnas en el docstring o mediante un contrato dedicado.
- Usar `Protocol` para puertos reemplazables.
- Usar `dataclass(frozen=True)` para configuración y valores inmutables.
- Usar `Enum` para conjuntos cerrados como tipos de medición o estado de lote.
- Evitar `dict[str, Any]` cuando un `dataclass` o `TypedDict` exprese mejor el
  contrato.
- No ocultar errores del tipado con `# type: ignore` sin justificación local.

## 5. Funciones

Las funciones deben ser cortas, tener una sola intención y hacer explícitas sus
dependencias:

```python
def normalize_column_name(column_name: str) -> str:
    """Return the canonical representation of one source column name."""
    normalized = column_name.replace("\n", " ").strip().lower()
    return normalized.replace(" ", "_")
```

Reglas:

- Preferir funciones puras para normalización y transformación.
- Evitar argumentos mutables como valores por defecto.
- No usar estado global mutable.
- No modificar argumentos recibidos sin documentarlo.
- Separar validación, transformación y persistencia en funciones diferentes.
- Evitar más de cinco o seis parámetros; usar un objeto de configuración cuando
  el grupo de parámetros forme un concepto.
- Evitar funciones de más de 40 líneas salvo que el flujo lineal sea más claro
  y esté cubierto por tests.
- Los nombres de funciones deben expresar el efecto: `filter_target_institutions`
  es preferible a `clean_data`.

## 6. Pandas y `DataFrame`

### 6.1 Contratos

Cada función que recibe o retorna un `DataFrame` debe documentar:

- Columnas requeridas.
- Tipos lógicos esperados.
- Si conserva el índice.
- Si modifica o copia el objeto.
- Grano de cada fila.

### 6.2 Mutabilidad

La preferencia es no mutar el objeto recibido en funciones reutilizables:

```python
def normalize_snies(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a normalized copy without mutating the input frame."""
    normalized = frame.copy()
    normalized.columns = [normalize_column_name(name) for name in normalized.columns]
    return normalized
```

Si copiar un conjunto grande tiene un coste medido, se puede mutar dentro de
una etapa controlada, pero debe quedar documentado y probado.

### 6.3 Operaciones seguras

- Preferir asignaciones con `.loc` para evitar `SettingWithCopyWarning`.
- No encadenar indexación y asignación.
- Usar `dropna` solo con columnas y política explícitas.
- Convertir tipos con `pd.to_numeric(..., errors="raise")` cuando un valor
  inválido debe detener el proceso; usar `coerce` solo si la política de
  rechazados lo define.
- Tratar códigos de institución, programa y municipio como `str`.
- No usar `astype(int)` antes de decidir qué hacer con nulos o valores inválidos.
- Usar `merge(..., validate=...)` cuando la cardinalidad sea conocida.
- Medir filas antes y después de cada `merge` que pueda descartar o duplicar
  registros.
- Seleccionar las columnas finales de forma explícita.
- Evitar `iterrows()` para transformar grandes volúmenes; preferir operaciones
  vectorizadas o procesamiento por lotes medido.
- No usar `inplace=True` como sustituto de un contrato claro.

### 6.4 Ejemplo de unión validada

```python
merged = facts.merge(
    dimension,
    left_on="codigo_institucion",
    right_on="codigo_ies",
    how="left",
    validate="many_to_one",
    indicator=True,
)

unmatched = merged["_merge"].eq("left_only").sum()
```

El conteo de `unmatched` debe incorporarse al reporte y no perderse en un
`print` aislado.

## 7. Errores y excepciones

Se deben distinguir tres situaciones:

1. **Dato inválido:** el registro incumple un contrato y se clasifica como
   rechazado o detiene el lote según la política.
2. **Ausencia esperada:** una consulta no encuentra una dimensión o un lote
   todavía no existe.
3. **Fallo técnico:** conexión caída, archivo ilegible o configuración inválida.

Reglas:

- Crear excepciones propias solo para errores que el llamador pueda manejar de
  forma distinta.
- Incluir archivo, etapa, columna, valor o clave y causa original en el error.
- No capturar `Exception` para continuar silenciosamente.
- Si se captura una excepción, registrar contexto y relanzar o convertirla en
  un resultado explícito.
- No usar excepciones para el flujo normal de "no hay filas".
- No exponer credenciales, cadenas de conexión ni datos sensibles en mensajes.

## 8. Logging y reportes

Se usará `logging` de la biblioteca estándar o un adaptador compatible. No se
usará `print` en código de producción.

```python
logger.info(
    "snies_transformation_completed",
    extra={"source": str(path), "input_rows": input_rows, "output_rows": output_rows},
)
```

Reglas:

- Usar mensajes estables y contexto estructurado.
- No interpolar contraseñas, tokens ni filas completas.
- `INFO`: inicio y resultado de etapas relevantes.
- `WARNING`: rechazos o faltantes recuperables.
- `ERROR`: fallo de una etapa o lote.
- `DEBUG`: diagnóstico detallado solo cuando sea necesario.
- Las métricas críticas deben existir en un reporte estructurado, no únicamente
  en logs.

## 9. Configuración y secretos

- Leer configuración en el borde de la aplicación.
- Pasar configuración explícitamente a los componentes que la necesitan.
- No leer `os.getenv()` repetidamente dentro de la lógica de negocio.
- Validar que las variables requeridas existen antes de abrir una conexión.
- No construir URLs de conexión con valores sin escapar; preferir la API de
  configuración de SQLAlchemy.
- `.env` nunca se versiona.
- `.env.example` contiene nombres y valores ficticios, nunca credenciales reales.

## 10. SQL y SQLAlchemy

- Toda consulta con datos externos debe usar parámetros.
- Las operaciones relacionadas deben usar una transacción explícita.
- Los nombres de tablas y columnas deben estar centralizados o documentados.
- No ejecutar SQL al importar un módulo.
- No crear un `Engine` global como efecto secundario de un import en código que
  deba probarse.
- Las consultas de lectura deben seleccionar solo las columnas necesarias.
- Las cargas deben informar filas afectadas y manejar conflictos según la
  política de idempotencia.
- Las consultas de control deben coincidir exactamente con el esquema DDL.
- Los cambios de esquema se documentan junto con migraciones o scripts SQL
  versionados.

## 11. Docstrings y comentarios

Los docstrings explican el contrato y el significado de los datos, no repiten
el nombre de la función. Deben incluir:

- Propósito.
- Parámetros no obvios.
- Retorno.
- Excepciones relevantes.
- Grano o columnas cuando se trabaja con `DataFrame`.

Los comentarios deben explicar por qué una regla existe. No deben narrar cada
línea ni conservar código muerto.

## 12. Complejidad y diseño de clases

- Una clase pública debe tener una responsabilidad cohesiva.
- Preferir composición a herencia.
- Mantener las dependencias en el constructor.
- No usar `__init__` para ejecutar cargas, consultas o lecturas costosas.
- Las clases de infraestructura deben poder reemplazarse por fakes en tests.
- Las transformaciones puras no necesitan convertirse en métodos de una clase.
- Separar métodos de lectura, validación y escritura cuando tengan efectos
  distintos.

## 13. Calidad mínima antes de integrar

Todo cambio de código debe cumplir, según corresponda:

- Formato Ruff sin cambios pendientes.
- Lint Ruff sin errores nuevos.
- Tipado sin errores nuevos en módulos tipados.
- Tests unitarios para transformaciones y reglas nuevas.
- Tests de integración para cambios de SQL o conexión.
- Sin secretos, datos temporales o archivos generados en el commit.
- Documentación actualizada cuando cambia un contrato de datos.

## 14. Anti-patrones

- `from config.database import engine` como dependencia oculta en cualquier
  función de dominio o transformación.
- `print()` para reportar estados de producción.
- `except Exception: pass` o `except Exception` seguido de `continue` sin
  registrar el lote rechazado.
- `df[col] = ...` sobre un subconjunto ambiguo.
- `errors="coerce"` sin contar ni clasificar los valores convertidos a nulo.
- `SELECT *` en código de persistencia.
- SQL construido con f-strings usando valores externos.
- Funciones llamadas `clean`, `process` o `handle` sin contrato específico.
- Variables globales mutables para rutas, conexión o estado de ejecución.
- Clases creadas únicamente para agrupar funciones sin estado.
- Tests que dependen de archivos personales o de una base de datos de producción.
