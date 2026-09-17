# Contexto del proyecto — ETL SNIES/SPADIES

## 1. Identidad del proyecto

Este repositorio implementa un proceso ETL para integrar información de
educación superior de Colombia proveniente de SNIES y SPADIES en PostgreSQL.
El sistema extrae archivos Excel y CSV, normaliza sus estructuras, valida la
calidad de los datos y carga dimensiones y tablas de hechos para su consulta
analítica.

El proyecto no es una aplicación CRUD ni un servicio web. Su unidad principal
de trabajo es el **dato transformado y trazable** desde una fuente de entrada
hasta una tabla de destino.

## 2. Rol del agente

El agente trabaja como científico de datos e ingeniero de datos responsable de:

- Comprender el significado estadístico y analítico de cada campo.
- Preservar la semántica de los datos durante la transformación.
- Hacer explícitas las reglas de limpieza, normalización y agregación.
- Detectar y reportar pérdida de registros, duplicados y claves no enlazadas.
- Mantener reproducibilidad entre ejecuciones.
- Separar lógica de negocio, procesamiento tabular, entrada/salida y persistencia.
- Proponer cambios verificables con tests y evidencia ejecutable.

El agente no debe inventar valores faltantes, corregir datos ambiguos de forma
silenciosa ni cambiar el grano de una tabla sin documentarlo y obtener
aprobación.

## 3. Fuentes de datos

### SNIES

Los archivos SNIES contienen indicadores académicos por institución,
programa, municipio, género, año, semestre y tipo de medición. Los tipos
identificados actualmente son:

- `inscritos`
- `admitidos`
- `matriculados`
- `graduados`

El flujo actual detecta el tipo a partir del nombre del archivo y busca de
forma dinámica la columna que contiene el valor de la medición.

### SPADIES

Los archivos SPADIES son CSV separados por punto y coma. Su estructura inicial
es ancha: una columna de categoría y varias columnas de periodo. El flujo las
convierte a formato largo con estas columnas:

- `variable`
- `categoria`
- `anio`
- `semestre`
- `porcentaje`

Actualmente se consideran periodos desde 2015.

### Dimensiones de referencia

El modelo utiliza dimensiones para relacionar los hechos con sus catálogos:

- Tiempo.
- Institución.
- Departamento.
- Municipio.
- Sexo o género.
- Programa académico.
- Oferta de programa por institución y municipio.
- Variable SPADIES.
- Categoría SPADIES.

## 4. Flujo lógico

Todo pipeline debe poder describirse con las siguientes etapas:

1. **Descubrir** archivos válidos y metadatos de origen.
2. **Extraer** datos sin alterar todavía su significado.
3. **Estandarizar** nombres de columnas y representaciones textuales.
4. **Transformar** al esquema canónico del proyecto.
5. **Validar** esquema, tipos, rangos, claves, duplicados y cardinalidad.
6. **Resolver dimensiones** y obtener las claves sustitutas necesarias.
7. **Cargar** dimensiones y hechos de forma transaccional cuando aplique.
8. **Registrar control** de ejecución, periodo, tipo de dato y resultado.
9. **Reportar** métricas, rechazos, advertencias y evidencia de la carga.

Una etapa no debe ocultar el fallo de otra. Si una transformación no puede
interpretar una columna requerida, el proceso debe fallar con un diagnóstico
accionable o enviar el registro a un flujo explícito de rechazados.

## 5. Grano y claves analíticas

Antes de modificar una transformación o carga, debe declararse el grano del
resultado. Para los hechos SNIES, el grano esperado combina la oferta del
programa, el tiempo, el género y el tipo de medición. Para los hechos SPADIES,
el grano combina categoría y tiempo.

Las claves de negocio deben conservarse durante las transformaciones hasta que
se hayan resuelto las claves sustitutas de la base de datos. No se debe usar
únicamente el índice del `DataFrame` como identificador de un registro.

## 6. Reglas de integridad de datos

- Los códigos de institución, programa y municipio se tratan como texto, no
  como números, para preservar ceros significativos.
- Año y semestre deben convertirse a tipos numéricos después de validar su
  contenido.
- Los porcentajes deben estar en un rango documentado antes de cargarse.
- Los valores nulos deben clasificarse como permitidos, rechazados o
  desconocidos; nunca deben eliminarse sin una razón registrada.
- Las uniones con dimensiones deben medir coincidencias y no coincidencias.
- Una unión `inner` que descarte datos requiere justificación y métricas.
- Las cargas repetidas deben ser idempotentes o detectar explícitamente que el
  periodo ya fue procesado.
- El conteo de registros debe compararse antes y después de cada etapa crítica.
- Los duplicados deben evaluarse con la clave natural del grano, no con todas
  las columnas por defecto.

## 7. Separación de responsabilidades

El diseño esperado distingue cuatro responsabilidades:

- **Dominio de datos:** contratos, nombres canónicos, reglas de negocio y
  modelos de configuración.
- **Aplicación:** coordinación del pipeline y orden de las etapas.
- **Infraestructura:** lectura de Excel/CSV, conexiones PostgreSQL y ejecución
  de consultas.
- **Interfaces:** scripts de entrada, parámetros y reporte de ejecución.

Las funciones de transformación deben ser preferentemente deterministas y
testeables sin conexión a la base de datos. Las clases se reservarán para
orquestadores, adaptadores, repositorios y componentes que necesiten mantener
un contrato o una dependencia.

## 8. Reproducibilidad y trazabilidad

Cada ejecución importante debe poder responder:

- Qué archivo se procesó.
- Qué tipo y periodo representaba.
- Qué versión de la transformación se utilizó.
- Cuántos registros entraron y salieron de cada etapa.
- Cuántos registros fueron rechazados y por qué.
- Qué dimensiones no pudieron resolverse.
- Qué registros se insertaron, actualizaron o ignoraron.
- Si la ejecución fue completa, parcial o fallida.

Las credenciales y parámetros de entorno deben permanecer fuera del código y
del control de versiones. Las muestras de configuración deben usar valores
ficticios.

## 9. Prioridades de ingeniería

Cuando existan tensiones entre objetivos, se aplicará este orden:

1. Correctitud y conservación de la semántica del dato.
2. Trazabilidad y capacidad de reproducir el resultado.
3. Validación y detección temprana de errores.
4. Idempotencia y seguridad de la carga.
5. Mantenibilidad y separación de responsabilidades.
6. Rendimiento, después de medir el cuello de botella.

No se debe optimizar una operación si antes no se puede demostrar que conserva
el resultado esperado.

## 10. Fuera de alcance implícito

Este contexto no autoriza por sí solo a:

- Cambiar tablas o columnas de producción.
- Modificar reglas estadísticas o de negocio no documentadas.
- Eliminar archivos de datos existentes.
- Reprocesar periodos sin confirmar la estrategia de idempotencia.
- Incorporar nuevas dependencias sin justificar su necesidad.
- Reestructurar todo el código en una sola entrega.

Cualquier cambio de alcance debe convertirse en una feature documentada y
aprobarse antes de implementarse.

## 11. Criterio de aceptación de este contexto

Este documento se considera aprobado cuando describe correctamente el propósito
del repositorio, las fuentes SNIES/SPADIES, el grano analítico, las reglas de
calidad, el flujo ETL y el rol esperado del agente. La aprobación de este
documento no aprueba todavía cambios de código ni la arquitectura detallada.
