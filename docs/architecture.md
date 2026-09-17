# Arquitectura — ETL Python SNIES/SPADIES

> Este documento define cómo se organiza el software de procesamiento de datos.
> La arquitectura prioriza correctitud, trazabilidad, reproducibilidad e
> idempotencia sobre abstracciones innecesarias.

## 1. Principios arquitectónicos

### 1.1 El dato es el centro del diseño

Cada módulo debe dejar claro qué esquema recibe, qué esquema produce y qué
reglas aplica. Una función o clase que transforma datos sin declarar sus
precondiciones y postcondiciones no tiene un contrato suficiente.

### 1.2 Separar cálculo de efectos secundarios

- Leer archivos y escribir en PostgreSQL son efectos secundarios.
- Normalizar columnas, mapear campos y validar reglas deben ser operaciones
  independientes de la infraestructura siempre que sea posible.
- Una transformación no debe abrir conexiones, leer archivos ni escribir en la
  base de datos.
- Una carga no debe decidir reglas de negocio que pertenecen a la transformación.

### 1.3 Programación estructurada por defecto

Las transformaciones tabulares se implementan preferentemente como funciones
pequeñas, secuenciales y deterministas:

1. Recibir un `DataFrame` y parámetros explícitos.
2. Validar precondiciones.
3. Ejecutar una transformación identificable.
4. Validar postcondiciones.
5. Retornar un nuevo resultado o un error explícito.

No se debe ocultar el flujo dentro de jerarquías de herencia ni de métodos con
efectos implícitos.

### 1.4 POO solo cuando resuelve un problema real

Se permiten clases cuando aportan al menos una de estas capacidades:

- Encapsular una dependencia externa.
- Sustituir una implementación por otra mediante un contrato.
- Mantener configuración o estado de una ejecución controlada.
- Representar una entidad o contrato con invariantes.
- Coordinar varias etapas como un caso de uso.

No se crearán clases para envolver una sola función, para imitar una arquitectura
enterprise ni para convertir cada `DataFrame` en un objeto artificial.

## 2. Capas lógicas

La separación se aplica aunque inicialmente algunos módulos continúen en las
rutas actuales.

```text
src/
├── domain/
│   ├── contracts/          # Esquemas, nombres canónicos y resultados
│   ├── models/             # Dataclasses y configuración de negocio
│   └── rules/              # Reglas puras de calidad y dominio
├── application/
│   ├── pipelines/          # Casos de uso SNIES, SPADIES y dimensiones
│   ├── services/           # Coordinación de etapas y métricas
│   └── ports/              # Protocolos para entrada, salida y control
├── infrastructure/
│   ├── readers/             # Excel, XLSB y CSV
│   ├── repositories/        # PostgreSQL y consultas de persistencia
│   ├── loaders/             # Adaptadores de carga por tabla
│   └── configuration/       # Entorno y conexiones
└── interfaces/
    ├── cli/                 # Entradas de ejecución
    └── reporting/           # Logs y reportes de ejecución
tests/
├── unit/
├── integration/
└── fixtures/
```

La estructura existente (`etl/`, `config/`, `main*.py`) puede migrarse de forma
incremental. No se debe hacer una reescritura completa solo para cumplir la
estructura de carpetas.

## 3. Responsabilidades por capa

### 3.1 Dominio de datos

El dominio define conceptos estables del problema:

- Tipo de medición SNIES.
- Periodo académico.
- Claves de negocio de institución, programa y municipio.
- Grano de hechos SNIES y SPADIES.
- Estados de ejecución y clasificación de rechazos.
- Reglas de rango, unicidad y obligatoriedad.

El dominio no debe importar pandas, SQLAlchemy, psycopg2, openpyxl ni librerías
de lectura. Si una regla necesita pandas por conveniencia, debe vivir en una
capa de validación de aplicación y tener una prueba que demuestre su contrato.

### 3.2 Aplicación

La aplicación coordina casos de uso completos:

- Procesar un archivo SNIES.
- Procesar un archivo SPADIES.
- Cargar dimensiones de referencia.
- Resolver claves de dimensiones.
- Registrar el control de una carga.

La aplicación conoce los puertos, pero no las implementaciones concretas de
Excel, CSV o PostgreSQL. Debe poder probarse usando lectores, repositorios y
registradores falsos.

### 3.3 Infraestructura

La infraestructura implementa los puertos definidos por la aplicación:

- Lectores de archivos.
- Repositorios y consultas parametrizadas.
- Cargadores por dimensión o hecho.
- Transacciones.
- Configuración de variables de entorno.

Los detalles de pandas y SQL deben estar confinados aquí o en transformadores
especializados de aplicación. Las consultas no deben dispersarse por los
scripts de entrada.

### 3.4 Interfaces

Los scripts de entrada solo deben:

- Leer argumentos o seleccionar una configuración.
- Crear dependencias.
- Invocar un caso de uso.
- Establecer el código de salida.

No deben contener reglas de normalización, `merge`, SQL ni decisiones de negocio.

## 4. Reglas de dependencia

```text
interfaces  ───────► application ───────► domain
     │                    │
     └──────────────► infrastructure ───┘
```

Reglas obligatorias:

1. `domain` no depende de ninguna otra capa.
2. `application` depende de `domain` y de sus propios puertos.
3. `infrastructure` implementa puertos; no define las reglas de negocio.
4. `interfaces` compone las dependencias y ejecuta casos de uso.
5. Ninguna transformación importa una conexión global creada al importar un
   módulo.
6. Ninguna capa debe importar un script `main` para reutilizar lógica.
7. Las dependencias de infraestructura se inyectan desde un único punto de
   composición.

## 5. Contratos de entrada y salida

Cada transformación debe documentar un contrato similar a este:

```text
Entrada: DataFrame SNIES crudo con columnas variables del proveedor.
Precondiciones: archivo legible y tipo de medición conocido.
Salida: DataFrame canónico con columnas y tipos definidos.
Postcondiciones: claves obligatorias presentes, periodo válido y sin columnas
                 auxiliares no autorizadas.
Errores: esquema no reconocido, columna requerida ausente o valor inválido.
```

Los contratos deben definir:

- Columnas obligatorias y opcionales.
- Tipo lógico de cada columna.
- Representación canónica de códigos y textos.
- Reglas de nulabilidad.
- Rango permitido.
- Grano esperado.
- Política para registros rechazados.

El nombre de una columna externa no es un contrato estable. La normalización y
el mapeo dinámico deben producir un esquema canónico antes de ejecutar reglas
de negocio o cargas.

## 6. Diseño de transformaciones

Una transformación compleja debe dividirse por intención, no por cantidad
arbitraria de líneas:

```python
def transform_snies(frame, measurement_type):
    normalized = normalize_columns(frame)
    mapped = map_source_columns(normalized, measurement_type)
    typed = cast_canonical_types(mapped)
    filtered = apply_business_filters(typed)
    validate_snies_frame(filtered)
    return select_fact_columns(filtered)
```

Cada función debe poder probarse por separado. Si una operación muta el
`DataFrame` recibido, debe estar explícito en su contrato; la preferencia es
crear copias en los límites donde se aplican cambios.

Las funciones no deben capturar `Exception` para continuar silenciosamente.
Cuando un archivo no puede procesarse, el pipeline debe conservar el contexto
del archivo, etapa, columna y valor problemático.

## 7. Diseño orientado a objetos

### 7.1 Puertos con `Protocol`

Para dependencias sustituibles se prefiere un protocolo pequeño:

```python
from pathlib import Path
from typing import Protocol

import pandas as pd


class TabularReader(Protocol):
    def read(self, path: Path) -> pd.DataFrame:
        """Read one source file into a raw DataFrame."""
```

La aplicación depende del contrato, no de `ExcelReader` o `CsvReader`.

### 7.2 Clases de servicio

Una clase de servicio debe tener una responsabilidad clara y dependencias
explícitas en su constructor. Ejemplos válidos:

- `SniesPipeline` coordina el caso de uso SNIES.
- `LoadControlRepository` consulta y registra ejecuciones.
- `PostgresFactLoader` persiste un hecho concreto.
- `ExecutionReport` acumula métricas de una ejecución.

Una clase de servicio no debe mezclar lectura de archivos, transformación,
validación y SQL en un único método.

### 7.3 Modelos

Para configuración y valores inmutables se prefieren `dataclass(frozen=True)`.
Para entidades con invariantes se permite una clase pequeña con métodos de
construcción y validación. No se debe crear una jerarquía de clases para cada
tipo de archivo.

### 7.4 Herencia

La herencia es excepcional. Se prefiere composición y protocolos porque los
lectores y cargadores deben poder sustituirse sin compartir estado implícito.
No se permiten jerarquías profundas ni clases base genéricas que oculten el
flujo del ETL.

## 8. Persistencia y cargas

### 8.1 Repositorios

Los repositorios encapsulan consultas y operaciones de persistencia. Deben:

- Usar parámetros, nunca interpolar valores en SQL.
- Exponer operaciones orientadas al caso de uso.
- Devolver resultados tipados o estructuras documentadas.
- Aislar nombres físicos de tablas de la lógica de transformación.
- Informar coincidencias, faltantes y filas afectadas.

### 8.2 Transacciones

Una unidad de carga que modifica tablas relacionadas debe usar una transacción
clara. Si el proceso tiene varias unidades independientes, cada unidad debe
definir si falla completa o admite rechazos parciales.

La política no puede inferirse del comportamiento accidental de `to_sql` o de
una conexión global.

### 8.3 Idempotencia

Cada carga debe declarar su clave natural y estrategia de repetición:

- Ignorar si el lote ya fue registrado.
- Insertar solo nuevas claves.
- Actualizar registros existentes.
- Reemplazar un lote completo dentro de una transacción.

La tabla de control debe representar el mismo grano que la política de
idempotencia. Una clave primaria incompleta o una columna de control ausente es
un error de diseño, no un detalle de implementación.

## 9. Patrones permitidos

### Adapter

Para encapsular Excel, CSV, PostgreSQL y proveedores externos. El resto del
sistema conoce un contrato propio y no el SDK concreto.

### Strategy

Para reglas intercambiables, por ejemplo detección de columnas según el tipo
SNIES o políticas de carga. Cada estrategia debe tener una interfaz pequeña y
tests propios.

### Pipeline

Para encadenar etapas explícitas de extracción, transformación, validación y
carga. Cada etapa debe conservar métricas y contexto.

### Repository

Para centralizar persistencia y consultas relacionadas con dimensiones, hechos y
control ETL. No debe convertirse en un acceso genérico a cualquier tabla.

### Null Object o resultado explícito

Para representar ausencia de datos o rechazos sin usar `None` ambiguo. La
elección debe documentar cómo se distingue "no encontrado", "nulo válido" y
"error de procesamiento".

## 10. Observabilidad de la ejecución

Cada caso de uso debe producir métricas mínimas:

- Archivo y origen.
- Tipo de pipeline.
- Registros leídos.
- Registros transformados.
- Registros rechazados.
- Registros con claves no resueltas.
- Registros insertados o actualizados.
- Duración por etapa.
- Estado final.

Se usará logging estructurado. Los mensajes no deben ser la única fuente de
control: las métricas importantes deben estar disponibles como datos de reporte
para poder probarlas.

## 11. Evolución desde el código actual

La migración recomendada es incremental:

1. Extraer funciones puras de normalización y transformación.
2. Añadir contratos y validaciones antes de modificar SQL.
3. Encapsular lectores y cargas detrás de puertos.
4. Crear servicios de aplicación para cada pipeline.
5. Centralizar la composición de dependencias.
6. Sustituir los scripts `main*.py` por interfaces delgadas.
7. Retirar estado global, `print` y consultas dispersas después de tener tests.

Cada paso debe conservar el resultado esperado o documentar explícitamente una
corrección de datos. No se deben mezclar refactorización arquitectónica y cambio
de reglas de negocio en la misma entrega sin aprobación.

## 12. Anti-patrones prohibidos

- Clase `DataProcessor` que hace todo el pipeline.
- Función que lee archivos, transforma datos y carga SQL simultáneamente.
- Conexión global creada al importar `config.database`.
- `except Exception` seguido de `print` y continuación silenciosa.
- `DataFrame` mutado por funciones cuyo contrato no lo declara.
- `merge` que descarta registros sin métrica ni justificación.
- SQL interpolado con valores de entrada.
- Herencia usada únicamente para reutilizar unas líneas.
- Métodos estáticos con configuración o estado mutable.
- Scripts de entrada que contienen reglas de negocio.
- Dependencia directa de pandas en el dominio conceptual.

## 13. Criterio de aceptación

Este documento se considera aprobado cuando establece una separación clara entre
transformaciones estructuradas, componentes orientados a objetos,
infraestructura y entradas del pipeline; además debe reflejar que la
correctitud y trazabilidad del dato son prioridades superiores a la cantidad de
abstracciones.
