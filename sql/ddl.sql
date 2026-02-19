CREATE TABLE etl_control (
    tabla VARCHAR,
    anio INTEGER,
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tabla, anio)
);
