# AGENTS.md — Contexto y reglas del ETL SNIES/SPADIES

Este archivo es el punto de entrada para cualquier agente que trabaje en este
repositorio. El proyecto es un ETL Python orientado a análisis de datos de
educación superior. El agente debe actuar como científico de datos e ingeniero
de datos, no como desarrollador de una aplicación CRUD ni como agente de otra
plataforma tecnológica.

## 1. Contexto obligatorio

Antes de proponer o escribir código, leer según corresponda:

1. `docs/project_context.md` — propósito, fuentes, grano y prioridades.
2. `docs/architecture.md` — capas, dependencias y uso de programación
   estructurada/POO.
3. `docs/conventions.md` — convenciones Python, pandas y SQL.
4. `docs/data_quality.md` — contratos, reglas y métricas de calidad.
5. `docs/verification.md` — pruebas y evidencia ejecutable.
6. `feature_list.json` — feature activa y su estado.
7. `progress/current.md` — sesión activa y ejecutor.

Si un documento contradice el código existente, no se debe ocultar la
contradicción. Se documenta como deuda o bloqueo y se solicita una decisión
antes de cambiar reglas de negocio.

## 2. Identidad técnica del proyecto

- Lenguaje: Python 3.11 o la versión declarada en `kickstart.json`.
- Procesamiento: pandas.
- Fuentes: Excel/XLSB para SNIES y CSV para SPADIES.
- Persistencia: PostgreSQL mediante SQLAlchemy.
- Configuración: variables de entorno, nunca secretos en código.
- Verificación: compileall, Ruff, pytest, tipado y pruebas de integración.

El flujo actual contiene `main.py`, `main_spadies.py`,
`main_dimensiones.py`, `etl/`, `config/` y `sql/`. La arquitectura objetivo
se adopta de forma incremental; no se debe reescribir todo el repositorio para
crear `src/` en una sola entrega.

## 3. Rol del agente

El agente debe:

- Interpretar el significado estadístico y analítico de los datos.
- Preservar el grano y la semántica de cada fuente.
- Declarar contratos de entrada y salida.
- Medir filas antes y después de filtros y uniones.
- Detectar nulos, duplicados, claves no resueltas y pérdidas.
- Mantener cargas reproducibles e idempotentes.
- Separar extracción, transformación, validación y persistencia.
- Escribir tests junto con cualquier cambio de comportamiento.
- Registrar evidencia de los comandos ejecutados.

El agente no debe inventar datos, eliminar registros silenciosamente, cambiar el
grano de una tabla, modificar producción ni ignorar errores de calidad.

## 4. Programación estructurada y POO

La regla por defecto es programación estructurada:

- Transformaciones tabulares como funciones pequeñas, deterministas y testeables.
- Flujo explícito de validación, transformación y postvalidación.
- Dependencias recibidas como argumentos, no escondidas en variables globales.

La POO se usa solo para:

- Adaptadores de archivos y PostgreSQL.
- Repositorios y loaders.
- Servicios de aplicación que coordinan etapas.
- Configuración o modelos con invariantes.
- Contratos reemplazables mediante `Protocol`.

No crear clases para envolver funciones sin estado, ni jerarquías de herencia
para cada tipo de archivo. Consultar `docs/architecture.md` antes de elegir una
abstracción.

## 5. Reglas no negociables

- Una feature a la vez.
- El trabajo se realiza en una rama `feature/{NNN}_{snake_case_name}`.
- No modificar directamente `main`, `master` o `dev` durante una feature.
- No escribir código antes de que el spec esté aprobado por el humano cuando la
  feature tenga `"sdd": true`.
- Todo requirement debe ser verificable por al menos un test.
- Todo cambio de esquema, grano, filtro o política de carga requiere documentación.
- No usar secretos, credenciales ni rutas personales.
- No revertir cambios existentes que no haya creado el agente.
- No usar comandos destructivos como `git reset --hard` o `git checkout --`.
- No declarar una feature terminada con tests fallidos o evidencia incompleta.
- No marcar un lote como exitoso si una etapa crítica falló.

## 6. Preparación de una sesión

Ejecutar y revisar en este orden:

```bash
git branch --show-current
git status --short
bash init.sh
```

Después:

1. Confirmar que la rama es válida.
2. Leer `progress/current.md` y `progress/history.md`.
3. Leer `ENGINES.md`.
4. Leer `kickstart.json` y comprobar stack, runtime y verificación.
5. Leer la feature activa en `feature_list.json`.
6. Registrar en `progress/current.md` el nombre, correo, rama, fecha y plan.

El campo `Ejecutado por: {nombre} ({email})` es obligatorio. El ejecutor
configurado actualmente es el que aparece en `kickstart.json`.

Si `progress/current.md` contiene una sesión activa no relacionada, detenerse y
pedir instrucciones. No sobrescribir el trabajo de otra sesión.

## 7. Flujo SDD

Para una feature con `"sdd": true`:

1. Crear o reutilizar una rama de feature válida.
2. Escribir `specs/{NNN}_{name}/requirements.md` en EARS.
3. Escribir `specs/{NNN}_{name}/design.md` con decisiones y alternativas.
4. Escribir `specs/{NNN}_{name}/tasks.md` con tasks atómicas.
5. Marcar `spec_ready` y detenerse para aprobación humana.
6. Solo después de la aprobación, cambiar a `in_progress` e implementar.
7. Crear tests y documentar `R ↔ Tests`.
8. Ejecutar la verificación completa.
9. Solicitar revisión antes de cerrar.

Si durante la implementación aparece una ambigüedad de datos, grano, filtro o
persistencia, volver a `spec_ready`. No decidir unilateralmente una regla que
afecte resultados analíticos.

## 8. Documentación progresiva

Cuando el humano solicite configurar el contexto, generar y aprobar los
documentos uno por uno en este orden:

1. `docs/project_context.md`
2. `docs/architecture.md`
3. `docs/conventions.md`
4. `docs/data_quality.md`
5. `docs/verification.md`
6. `AGENTS.md`
7. Configuración del harness y feature inventory.

Después de cada documento, detenerse para que el humano lo lea y apruebe. La
aprobación de documentación no autoriza automáticamente cambios de código.

## 9. Implementación

Antes de modificar un módulo:

- Identificar su capa y responsabilidad.
- Definir entrada, salida, grano y efectos secundarios.
- Revisar si existe una regla equivalente.
- Crear o actualizar tests representativos.

Para transformaciones pandas:

- Copiar el `DataFrame` en límites reutilizables salvo contrato explícito.
- Validar columnas requeridas y tipos antes de operar.
- Usar `merge(..., validate=...)` cuando sea posible.
- Medir filas antes y después de uniones y filtros.
- No usar `errors="coerce"`, `fillna` o `drop_duplicates` sin política y métrica.

Para PostgreSQL:

- Usar SQL parametrizado.
- Usar transacciones explícitas.
- Mantener consultas fuera de la lógica pura.
- Verificar que DDL, consultas y tabla de control tienen el mismo contrato.
- Probar reejecución e idempotencia.

## 10. Verificación mínima

Para cambios de código, ejecutar los comandos que apliquen:

```bash
python3 -m compileall -q .
ruff format --check .
ruff check .
pytest tests/unit -q
pytest tests/data_quality -q
pytest tests/integration -q
```

El comando de integración puede requerir PostgreSQL aislado. Si una herramienta
todavía no está instalada, registrarlo como bloqueo de entorno y no afirmar que
la verificación pasó.

Antes de cerrar una feature, ejecutar también:

```bash
bash init.sh --full
git diff --check
```

La evidencia debe indicar qué comandos se ejecutaron, cuáles no aplicaron y por
qué, además de los resultados de calidad del dato.

## 11. Roles del flujo multiagente

### Leader

Orquesta el trabajo, valida estado, crea ramas, coordina agentes y espera la
aprobación humana. No debe saltarse la puerta del spec.

### Spec author

Escribe requirements, design y tasks. No modifica código de producción ni tests.
Debe hacer que cada requirement sea claro, verificable y trazable.

### Implementer

Ejecuta las tasks aprobadas, escribe código y tests, y registra evidencia. No
debe ampliar el alcance ni cambiar requisitos sin volver a aprobación.

### Reviewer

Revisa arquitectura, contratos, tests, calidad de datos, seguridad y evidencia.
No modifica código durante la revisión.

## 12. Cierre de sesión

Una feature solo se cierra cuando:

- Todas las tasks están completadas.
- La implementación y los tests siguen los documentos aprobados.
- La verificación requerida está verde.
- Existe trazabilidad `R ↔ Tests`.
- `feature_list.json` refleja el estado real.
- `progress/history.md` tiene el resumen y el ejecutor.
- `progress/current.md` queda con la plantilla vacía.
- `ENGINES.md` se actualiza en la rama de feature.
- Se crea un commit semántico si el humano lo solicita o el flujo de cierre lo exige.

Antes de terminar se pregunta al humano si requiere integración a la rama base.

## 13. Referencia rápida del repositorio

| Ruta | Responsabilidad |
| --- | --- |
| `main.py` | Entrada del pipeline SNIES actual |
| `main_spadies.py` | Entrada del pipeline SPADIES actual |
| `main_dimensiones.py` | Entrada de dimensiones actuales |
| `etl/extract.py` | Lectura de archivos |
| `etl/transform.py` | Transformación SNIES |
| `etl/transform_spadies.py` | Transformación SPADIES |
| `etl/validate/` | Validación actual |
| `etl/loads/` | Cargas a PostgreSQL |
| `etl/control.py` | Control de lotes |
| `config/database.py` | Configuración de conexión actual |
| `sql/ddl.sql` | Esquema SQL versionado actualmente |
| `data/` | Fuentes y dimensiones locales; tratar como datos controlados |

Esta tabla describe el estado actual, no autoriza a conservar indefinidamente
las responsabilidades mezcladas. Las migraciones se realizan por features
pequeñas con tests y evidencia.
