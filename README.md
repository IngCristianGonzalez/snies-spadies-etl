# ETL SNIES/SPADIES

Pipeline Python para integrar datos de educación superior de Colombia desde
SNIES y SPADIES hacia PostgreSQL. El repositorio usa un harness multiagente con
Spec Driven Development (SDD), trazabilidad y controles de calidad de datos.

## Propósito

El proyecto extrae archivos Excel/XLSB y CSV, normaliza sus estructuras,
transforma los datos a un modelo analítico, valida su calidad y carga
dimensiones y hechos en PostgreSQL.

El agente trabaja como científico de datos e ingeniero ETL. La prioridad es
preservar el significado, el grano, la reproducibilidad y la trazabilidad de
los datos.

## Fuentes y pipelines

- **SNIES:** inscritos, admitidos, matriculados y graduados.
- **SPADIES:** indicadores en formato ancho convertidos a formato largo.
- **Dimensiones:** tiempo, instituciones, municipios, departamentos, sexo,
  programas y oferta académica.
- **Destino:** PostgreSQL mediante SQLAlchemy.

## Inicio rápido

```bash
cp kickstart.json.example kickstart.json
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements-dev.txt
bash init.sh
```

Completa las variables de `.env.example` en un `.env` local. No versiones
credenciales ni archivos de datos privados.

Las herramientas de desarrollo requeridas para la verificación son Ruff,
pytest y una herramienta de tipado estático. Deben instalarse en el entorno
virtual del proyecto antes de ejecutar `bash init.sh --full`.

## Estructura actual

```text
.
├── AGENTS.md                  # Contexto y reglas para agentes
├── config/                    # Configuración de conexión actual
├── data/                      # Fuentes y dimensiones controladas
├── etl/
│   ├── extract.py             # Lectura de archivos
│   ├── transform.py           # Transformación SNIES
│   ├── transform_spadies.py   # Transformación SPADIES
│   ├── validate/              # Validaciones actuales
│   └── loads/                 # Cargas a PostgreSQL
├── main.py                    # Entrada SNIES
├── main_spadies.py            # Entrada SPADIES
├── main_dimensiones.py        # Entrada de dimensiones
├── sql/                       # DDL versionado
├── docs/                      # Contexto, arquitectura y verificación
├── progress/                  # Estado y evidencia de sesiones
└── tests/                     # Tests unitarios, calidad e integración
```

La arquitectura objetivo separa `domain`, `application`, `infrastructure` e
`interfaces`, pero la migración será incremental. No se reescribe todo el
repositorio para crear `src/` en una sola entrega.

## Documentación obligatoria

| Documento | Propósito |
| --- | --- |
| `AGENTS.md` | Reglas operativas para cualquier agente |
| `docs/project_context.md` | Fuentes, grano y rol del proyecto |
| `docs/architecture.md` | Capas, programación estructurada y POO |
| `docs/conventions.md` | Python, pandas, SQL y estilo |
| `docs/data_quality.md` | Contratos y métricas de calidad |
| `docs/verification.md` | Tests y evidencia ejecutable |
| `docs/specs.md` | Flujo SDD y trazabilidad |
| `feature_list.json` | Inventario y estado de features |
| `progress/current.md` | Sesión activa |
| `progress/history.md` | Historial append-only |

## Flujo SDD

Cada feature se trabaja en su propia rama y sigue este ciclo:

```text
leader → spec-author → aprobación humana → implementer → reviewer → cierre
```

El spec contiene:

- `requirements.md`: qué se necesita, en EARS.
- `design.md`: cómo se implementará, con ADRs.
- `tasks.md`: pasos atómicos y trazables.

No se escribe código de una feature con `sdd: true` antes de la aprobación
humana del spec.

## Verificación

```bash
python3 -m compileall -q .
ruff format --check .
ruff check .
pytest tests/unit -q
pytest tests/data_quality -q
pytest tests/integration -q
bash init.sh --full
```

La verificación debe registrar filas de entrada y salida, rechazos, duplicados,
claves no resueltas, cargas y resultado de idempotencia. Si una herramienta no
está instalada, se reporta como bloqueo y no como resultado exitoso.

## Reglas de diseño

- Transformaciones tabulares: funciones pequeñas, deterministas y testeables.
- POO: solo para adaptadores, repositorios, loaders, configuración y orquestación.
- SQL parametrizado y transacciones explícitas.
- Ninguna pérdida de registros sin política y métrica.
- Ningún secreto en código o documentación versionada.
- Ningún lote se marca exitoso si una etapa crítica falló.
