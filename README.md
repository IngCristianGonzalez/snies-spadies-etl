# SNIES & SPADIES ETL

Pipeline ETL para datos de educación superior Colombiana. Extrae archivos Excel/CSV de las fuentes **SNIES** (matrículas, inscritos, admitidos, graduados) y **SPADIES** (deserción), los transforma a un modelo dimensional y los carga en PostgreSQL para su análisis en un tablero de control.

## Arquitectura

```
app.py
├── main_dimensiones.py   → Carga tablas dimensionales desde data/dimensions/*.xlsx
├── main.py               → Pipeline SNIES (hechos)
└── main_spadies.py       → Pipeline SPADIES (hechos)

Cada pipeline sigue 5 etapas:
  Extract → Transform → Validate → Load → Control (auditoría)
```

## Tecnologías

| Tecnología | Versión | Uso |
|---|---|---|
| Python | 3.11+ | Lenguaje principal |
| pandas | 2.x | Manipulación de datos, lectura Excel/CSV |
| SQLAlchemy | 2.x | Conexión a BD y ejecución SQL |
| psycopg2-binary | - | Driver PostgreSQL |
| openpyxl | - | Lectura de archivos .xlsx |
| pyxlsb | - | Lectura de archivos .xlsb |
| python-dotenv | - | Configuración por variables de entorno |
| PostgreSQL | 15 | Base de datos destino |
| Docker | - | Contenedores y despliegue |

## Estructura del proyecto

```
snies-spadies-etl/
├── app.py                     # Orquestador: ejecuta los 3 pipelines en secuencia
├── main.py                    # Pipeline SNIES
├── main_dimensiones.py        # Carga de dimensiones estáticas
├── main_spadies.py            # Pipeline SPADIES
├── requirements.txt           # Dependencias Python
├── dockerfile                 # Construcción multi-etapa
├── docker-compose.yml         # Servicios db + app
├── .env.example               # Template de configuración
│
├── config/
│   └── database.py            # Motor SQLAlchemy (engine)
│
├── etl/
│   ├── extract.py             # Lectura de Excel (xlsx, xlsb) y CSV
│   ├── transform.py           # Transformaciones SNIES
│   ├── transform_spadies.py   # Transformaciones SPADIES
│   ├── control.py             # Registro de carga en etl_control
│   ├── validate/
│   │   └── validate.py        # Validaciones de integridad y estadísticas
│   └── loads/
│       ├── load_dim_departamentos.py
│       ├── load_dim_institucion.py
│       ├── load_dim_municipio.py
│       ├── load_dim_municipios.py
│       ├── load_dim_programa.py
│       ├── load_dim_programa_oferta.py
│       ├── load_dim_sexo.py
│       ├── load_dim_tiempo.py
│       ├── load_facts.py      # Carga de tb_fact_snies
│       └── load_spadies.py    # Carga de dimensiones y hecho SPADIES
│
├── data/
│   ├── dimensions/            # Archivos semilla para tablas dimensionales
│   └── snies/                 # Datos SNIES organizados por año
│   └── spadies/               # Datos SPADIES en CSV
│
└── sql/
    └── ddl.sql                # DDL de la tabla de auditoría etl_control
```

## Flujo ETL

### SNIES (`main.py`)

```
data/snies/<año>/<tipo>.xlsx
       ↓
extract.py → extract_excel()  → Lee Excel, detecta fila de encabezados
       ↓
transform.py → transformar_snies()
  ├── clean_snies()            → Normaliza nombres de columnas (NFKD, lowercase)
  ├── detectar_columna_valor() → Encuentra columna de valor por tipo
  ├── mapear_columnas()        → Mapea a esquema canónico
  └── filter_instituciones()   → Filtra códigos IES 1120 y 1123
       ↓
validate.py → validate_snies_generic() → Estadísticas de control
       ↓
load_dim_programa_oferta.py    → Upsert de oferta programa-institución-municipio
       ↓
load_facts.py                  → Merge contra dimensiones, dedup, INSERT masivo
       ↓
control.py → register_year()  → Registro en etl_control
```

### SPADIES (`main_spadies.py`)

```
data/spadies/<archivo>.csv
       ↓
pd.read_csv(sep=";", encoding="utf-8-sig")
       ↓
transform_spadies.py → transformar_spadies()
  ├── Limpia BOM de nombres
  ├── Unpivot (wide → long)
  ├── Parsea período (YYYY-S → año + semestre)
  └── Filtra >= 2015
       ↓
load_spadies.py → Carga row-by-row
  ├── tb_dim_variable_spadies (upsert)
  ├── tb_dim_categoria_spadies (insert/select)
  ├── tb_dim_tiempo (lookup)
  └── tb_fact_spadies (upsert)
```

## Modelo de datos (Esquema en estrella)

```
tb_dim_departamento ─┐
                     ├── tb_dim_municipio ──┐
tb_dim_institucion ──┘                      │
                                            ├── tb_dim_programa_oferta ──┐
tb_dim_programa ────────────────────────────┘                            │
                                                                         ├── tb_fact_snies
tb_dim_sexo ─────────────────────────────────────────────────────────────┘
tb_dim_tiempo ───────────────────────────────────────────────────────────┘

tb_dim_variable_spadies ── tb_dim_categoria_spadies ── tb_fact_spadies
                                                          │
                                                     tb_dim_tiempo
```

## Buenas prácticas observadas

- **Separación por etapas**: extract, transform, load, validate y control en módulos independientes.
- **Cargas idempotentes**: las dimensiones verifican existencia antes de insertar; los hechos evitan duplicados contra etl_control.
- **Configuración por entorno**: credenciales vía .env + python-dotenv.
- **Auditoría**: tabla etl_control registra qué año/tabla se cargó y cuándo.
- **Dockerizado**: reproducible con docker-compose (PostgreSQL + app).
- **Detección dinámica de columnas**: localiza la columna de valor por contenido, no por posición.
- **Normalización consistente**: todos los loaders limpian códigos (strip, str, sin .0) antes de hacer joins.
- **Unicode**: transform.py usa unicodedata NFKD para manejar caracteres acentuados.
- **Validación**: validate.py imprime estadísticas resumen para control manual.

## Ejecución

```bash
# Instalar dependencias
pip install -r requirements.txt

# Configurar variables de entorno (copiar .env.example → .env)
cp .env.example .env

# Ejecutar pipeline completo
python app.py

# O por partes
python main_dimensiones.py   # Solo dimensiones
python main.py               # Solo SNIES
python main_spadies.py       # Solo SPADIES

# Con Docker
docker-compose up --build
```
