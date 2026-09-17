"""Normalización pura de la dimensión sexo y mapeo de códigos de género SNIES.

Este módulo no depende de infraestructura (base de datos, lectores ni
configuración). Cada función es determinista, no tiene efectos secundarios y
recibe `pandas.DataFrame` o `pandas.Series` para devolver su equivalente
canónico. Cumple los requirements R1-R7 de `specs/015_dimension_sexo/`.
"""

from __future__ import annotations

import unicodedata

import pandas as pd

SIN_INFORMACION_CODE: int = 9
SIN_INFORMACION_LABEL: str = "SIN INFORMACION"
ALLOWED_GENERO_CODES: frozenset[int] = frozenset({1, 2, 3, 4})

METRIC_COLUMNS = (
    "stage",
    "source",
    "input_rows",
    "output_rows",
    "rejected_rows",
    "null_rows",
    "duplicate_rows",
    "unmatched_rows",
    "inserted_rows",
    "updated_rows",
    "affected_genero_rows",
    "duration_seconds",
    "status",
)


class SexoContractError(ValueError):
    """Error de contrato de la dimensión sexo con contexto de los registros afectados."""

    def __init__(self, message: str, conflicts: list[str]) -> None:
        super().__init__(message)
        self.conflicts = conflicts


def normalize_description(value: object) -> str:
    """Return the display description normalized: external spaces removed and uppercase."""
    return str(value).strip().upper()


def description_key(value: object) -> str:
    """Return an accent-insensitive key used only for comparison, not for display."""
    normalized = unicodedata.normalize("NFKD", normalize_description(value))
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def validate_sexo_contract(frame: pd.DataFrame) -> list[str]:
    """Return contract violations of the sexo dimension input (empty list if valid).

    No modifica el `DataFrame`. Verifica columnas obligatorias, `codigo` nulo o
    no numérico, `descripcion` nula y `codigo` duplicado con descripciones
    distintas.
    """
    missing = [col for col in ("codigo", "descripcion") if col not in frame.columns]
    if missing:
        return [f"faltan columnas obligatorias: {', '.join(missing)}"]

    work = frame.copy()
    work["_codigo"] = pd.to_numeric(work["codigo"], errors="coerce")
    work["_key"] = [description_key(value) for value in work["descripcion"]]

    conflicts: list[str] = []
    for index in work.index[work["_codigo"].isna()].tolist():
        conflicts.append(f"codigo nulo o no numérico en fila {int(index)}")
    for index in work.index[work["descripcion"].isna()].tolist():
        conflicts.append(f"descripcion nula en fila {int(index)}")

    valid = work["_codigo"].notna() & work["descripcion"].notna()
    grouped = work.loc[valid].groupby("_codigo")["_key"].nunique()
    for code in grouped.index[grouped > 1].tolist():
        conflicts.append(f"codigo {int(code)} con descripciones distintas")
    return conflicts


def normalize_sexo_dimension(
    frame: pd.DataFrame,
    *,
    include_sin_informacion: bool = True,
) -> pd.DataFrame:
    """Return the canonical sexo dimension for a catalog frame.

    Entrada: `DataFrame` con columnas obligatorias `codigo` y `descripcion`.
    Salida: `DataFrame` con `codigo` (`int64`) y `descripcion` (`str`, UPPER +
    strip), único por `codigo` y ordenado, que incluye `SIN INFORMACION`
    (código 9) cuando el origen no la traiga.

    Eleva `SexoContractError` si el contrato no se cumple (R2).
    """
    conflicts = validate_sexo_contract(frame)
    if conflicts:
        detail = "; ".join(conflicts)
        raise SexoContractError(
            f"Dimensión sexo con errores de contrato: {detail}", conflicts
        )

    normalized = frame[["codigo", "descripcion"]].copy()
    normalized["codigo"] = pd.to_numeric(normalized["codigo"], errors="raise").astype(
        "int64"
    )
    normalized["descripcion"] = normalized["descripcion"].apply(normalize_description)
    normalized = normalized.drop_duplicates(subset="codigo", keep="first")

    if include_sin_informacion and SIN_INFORMACION_CODE not in set(
        normalized["codigo"]
    ):
        extra = pd.DataFrame(
            [{"codigo": SIN_INFORMACION_CODE, "descripcion": SIN_INFORMACION_LABEL}]
        )
        normalized = pd.concat([normalized, extra], ignore_index=True)

    normalized["codigo"] = normalized["codigo"].astype("int64")
    return normalized.sort_values("codigo").reset_index(drop=True)


def map_genero_codes(
    series: pd.Series,
    *,
    allowed_codes: frozenset[int] = ALLOWED_GENERO_CODES,
    replacement: int = SIN_INFORMACION_CODE,
) -> tuple[pd.Series, int]:
    """Map null, zero or invalid `id_genero` values to `SIN INFORMACION`.

    Devuelve una tupla `(códigos_enteros, afectados)` donde `afectados` es la
    cantidad de registros mapeados. Un valor se considera inválido si no está
    en `allowed_codes` (incluye nulos, texto no numérico y el código 0). No
    muta la serie recibida.
    """
    codes = pd.to_numeric(series.copy(), errors="coerce")
    invalid = ~codes.isin(allowed_codes)
    affected = int(invalid.sum())
    mapped = codes.where(~invalid, other=replacement).astype("int64")
    return mapped, affected


def sexo_dimension_metrics(
    *,
    source: str,
    input_rows: int,
    output_rows: int,
    rejected_rows: int = 0,
    null_rows: int = 0,
    duplicate_rows: int = 0,
    unmatched_rows: int = 0,
    inserted_rows: int = 0,
    updated_rows: int = 0,
    affected_genero_rows: int = 0,
    duration_seconds: float = 0.0,
    status: str = "ok",
) -> pd.DataFrame:
    """Return a single-row metrics report following the project quality schema.

    La fila conserva el esquema `METRIC_COLUMNS` de `docs/data_quality.md` para
    que la evidencia sea reproducible y no dependa de `print`.
    """
    return pd.DataFrame(
        [
            {
                "stage": "transform_sexo",
                "source": source,
                "input_rows": input_rows,
                "output_rows": output_rows,
                "rejected_rows": rejected_rows,
                "null_rows": null_rows,
                "duplicate_rows": duplicate_rows,
                "unmatched_rows": unmatched_rows,
                "inserted_rows": inserted_rows,
                "updated_rows": updated_rows,
                "affected_genero_rows": affected_genero_rows,
                "duration_seconds": duration_seconds,
                "status": status,
            }
        ]
    )
