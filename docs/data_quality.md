# Calidad de datos — SNIES/SPADIES

> La calidad no es una revisión al final del ETL. Es un conjunto de contratos
> medibles que protegen el significado, el grano y la trazabilidad del dato en
> cada etapa.

## 1. Principios

- Ningún registro se descarta silenciosamente.
- Toda conversión de tipos tiene una política explícita para valores inválidos.
- Toda unión con una dimensión mide coincidencias y faltantes.
- Todo cambio de cardinalidad se explica con métricas.
- Las validaciones se ejecutan antes de escribir datos persistentes.
- Un lote inválido no debe marcarse como cargado correctamente.
- Las reglas de calidad deben poder ejecutarse sin PostgreSQL cuando no
  dependan de persistencia.
- Una advertencia no sustituye un registro de rechazo ni una métrica.

## 2. Niveles de validación

Las validaciones se ejecutan en orden:

1. **Archivo:** existencia, extensión, tamaño y legibilidad.
2. **Estructura:** columnas, encabezados y formato del origen.
3. **Tipos:** representación de códigos, años, semestres y medidas.
4. **Contenido:** nulos, rangos, dominios permitidos y textos.
5. **Grano:** unicidad y duplicados según la clave natural.
6. **Referencial:** resolución contra dimensiones.
7. **Carga:** filas afectadas, conflictos e idempotencia.
8. **Conciliación:** comparación entre entrada, salida y destino.

Una etapa posterior no debe ocultar el fallo de una etapa anterior. Por ejemplo,
un `merge` no puede considerarse correcto solo porque produjo un `DataFrame`.

## 3. Contrato de archivo

Antes de transformar un archivo se debe registrar:

- Ruta o identificador del archivo.
- Nombre original.
- Extensión y tamaño.
- Fecha de modificación, si está disponible.
- Fuente y pipeline esperado.
- Tipo de medición o variable inferida.
- Periodos presentes, si pueden identificarse.
- Hash del archivo cuando la reproducibilidad lo requiera.

Archivos temporales de Excel, como los que empiezan por `~$`, no son entradas
válidas. Una extensión conocida no garantiza que el contenido tenga el formato
esperado; la lectura debe validar encabezados y columnas.

## 4. Contrato canónico SNIES

Después de la extracción y normalización, un registro SNIES debe tener como
mínimo:

| Campo | Tipo lógico | Regla |
| --- | --- | --- |
| `codigo_de_la_institucion` | texto | Obligatorio, conserva ceros |
| `codigo_snies_del_programa` | texto | Obligatorio, conserva identidad |
| `codigo_del_municipio_programa` | texto | Obligatorio, formato canónico |
| `anio` | entero | Obligatorio, periodo válido |
| `semestre` | entero | Obligatorio, dominio permitido |
| `tipo` | categoría | Tipo SNIES soportado |
| `valor` | numérico | No negativo salvo regla aprobada |
| `id_genero` o `genero` | categoría | Esquema definido, no implícito |

El contrato debe resolver la diferencia entre archivos que contienen `id_genero`
y archivos que contienen una descripción textual de género. No se debe acceder a
`id_genero` si el esquema canónico no garantiza que existe.

## 5. Grano SNIES

El grano lógico de los hechos SNIES es:

```text
oferta de programa + periodo + género + tipo de medición
```

Antes de resolver claves sustitutas debe validarse una combinación equivalente a:

```text
codigo_snies_del_programa
codigo_de_la_institucion
codigo_del_municipio_programa
anio
semestre
genero
tipo
```

Si el modelo usa `programa_oferta_id`, `tiempo_id` y `genero_id`, debe demostrarse
que esas claves representan exactamente el mismo grano.

Los duplicados se reportan con la clave natural, el conteo y una muestra de
claves afectadas. Nunca se eliminan con `drop_duplicates` sin definir cuál
registro es correcto y por qué.

## 6. Contrato canónico SPADIES

Después de convertir el archivo ancho a formato largo, el resultado debe tener:

| Campo | Tipo lógico | Regla |
| --- | --- | --- |
| `variable` | texto | Normalizada y no vacía |
| `categoria` | texto | No vacía después de normalizar |
| `anio` | entero | Periodo válido y dentro del alcance |
| `semestre` | entero | Dominio permitido |
| `porcentaje` | decimal | Rango definido, normalmente 0 a 100 |

Un periodo que no respete el formato esperado produce un rechazo con el valor
original. No se convierte silenciosamente a nulo.

## 7. Dimensiones

Las dimensiones son datos de referencia y tienen requisitos adicionales:

- La clave de negocio debe ser única o tener una regla aprobada.
- Los códigos deben tener representación canónica antes de relacionarse.
- Los catálogos deben cargarse antes de los hechos que los referencian.
- Una dimensión no debe duplicar claves por ejecutar el proceso varias veces.
- La carga informa filas nuevas, existentes y conflictivas.
- Los registros sin correspondencia se clasifican por dimensión.

Para `programa_oferta`, la combinación esperada es:

```text
programa + institución + municipio
```

Una fila duplicada en esa combinación compromete la resolución de hechos y debe
fallar la validación de la dimensión.

## 8. Reglas de tipos y normalización

### 8.1 Códigos

- Leer códigos como texto cuando sea posible.
- Eliminar espacios externos y representaciones como `.0` solo si provienen de
  una lectura numérica errónea comprobada.
- No aplicar `zfill` sin confirmar la longitud oficial.
- Registrar códigos vacíos, nulos o fuera de formato.

### 8.2 Texto

- Normalizar espacios y saltos de línea.
- Normalizar acentos solo para claves de comparación cuando sea necesario.
- Conservar el valor original cuando tenga valor analítico.
- No eliminar números de un texto sin documentar por qué no son semánticos.

### 8.3 Nulos

Cada columna debe declarar una política:

- `required`: un nulo rechaza el registro o lote.
- `optional`: un nulo es válido y se conserva.
- `defaulted`: se reemplaza por valor definido y se contabiliza.
- `unknown`: se conserva como categoría explícita.

`fillna(0)`, `fillna(3)` o `errors="coerce"` requieren justificación de dominio
y una métrica de cuántos valores fueron afectados.

## 9. Validaciones de relaciones

Toda relación entre hechos y dimensiones genera:

- Filas de entrada y después de la unión.
- Filas con clave resuelta y sin clave resuelta.
- Claves faltantes únicas.
- Duplicación producida por la unión.

Cuando se conozca la cardinalidad, debe declararse en la operación. Una
dimensión consultada por código normalmente debe cumplir `many_to_one` desde el
hecho.

Las políticas para una clave no resuelta son:

1. Rechazar el lote completo.
2. Separar el registro en rechazados y continuar.
3. Cargar a una zona pendiente de resolución.
4. Crear la dimensión automáticamente solo con una regla aprobada.

La política debe estar definida por pipeline y no elegirse dentro de un
`merge` accidental.

## 10. Conteos y conciliación

Cada etapa crítica debe registrar al menos:

```text
stage, source, input_rows, output_rows, rejected_rows,
null_rows, duplicate_rows, unmatched_rows, inserted_rows,
updated_rows, duration_seconds, status
```

Las conciliaciones obligatorias cuando apliquen son:

- Entrada extraída contra salida normalizada.
- Salida normalizada contra salida validada.
- Salida validada contra filas preparadas para carga.
- Filas preparadas contra filas insertadas o actualizadas.
- Suma de valores antes y después de transformaciones que no alteren magnitudes.
- Cantidad de claves únicas antes y después de una unión.

Todo cambio debe explicar si se debe a filtro aprobado, duplicación, rechazo,
deduplicación o falta de dimensión.

## 11. Rechazos y cuarentena

Un registro rechazado conserva:

- Archivo y pipeline.
- Número de fila o identificador de origen, si existe.
- Etapa y código de regla.
- Mensaje técnico legible.
- Valores relevantes, sin secretos.
- Fecha y ejecución que lo produjo.

Los rechazos no se confunden con errores técnicos. Un archivo ilegible o una
base de datos inaccesible marca la ejecución como fallida, no como un conjunto de
registros rechazados.

## 12. Severidad y umbrales

Cada regla declara severidad:

- **Error:** impide cargar el lote o la etapa.
- **Warning:** permite continuar, pero queda registrado.
- **Info:** métrica descriptiva sin impacto de calidad.

No se fijan porcentajes universales sin conocer la distribución real de las
fuentes. Los umbrales se definen por fuente, tipo de archivo y periodo, con
base en datos observados y aprobación del responsable del dominio.

Siempre son errores:

- Columnas obligatorias ausentes.
- Tipo de archivo ilegible.
- Periodo imposible de interpretar.
- Clave natural inválida para el grano.
- Duplicados en dimensiones que deben ser únicas.
- Violación de restricciones de base de datos.
- Carga marcada como exitosa sin evidencia de persistencia.

## 13. Idempotencia y control de lotes

El control de ejecución debe registrar la combinación que determina si un lote
puede repetirse. Según el pipeline puede incluir:

- Tabla o hecho destino.
- Fuente o archivo.
- Año y semestre.
- Tipo de medición o variable.
- Identificador de ejecución.
- Estado final.

Un lote solo se registra como completado después de carga y conciliación
exitosas. Los lotes fallidos o parciales se distinguen de los completados y se
reintentan según una política explícita.

## 14. Pruebas de calidad

Cada regla debe tener una prueba con datos pequeños y representativos:

- Columnas válidas e inválidas.
- Encabezado desplazado o ausente.
- Código con ceros significativos.
- Nulo obligatorio y nulo permitido.
- Periodo inválido.
- Porcentaje fuera de rango.
- Duplicado en el grano.
- Dimensión sin correspondencia.
- Unión que duplica registros.
- Reejecución del mismo lote.
- Archivo vacío o ilegible.

Las pruebas verifican el resultado, las métricas y el motivo del rechazo. Probar
únicamente que una función no lanza una excepción no demuestra calidad.

## 15. Criterio de aceptación

Este documento se considera aprobado cuando define contratos verificables para
SNIES, SPADIES y dimensiones; establece cómo detectar pérdidas, duplicados y
claves no resueltas; y diferencia advertencias, rechazos y fallos técnicos.
