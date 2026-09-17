from __future__ import annotations

import logging
import math
import re
from numbers import Integral, Real
from pathlib import Path

import pandas as pd

from etl.exceptions import DimensionTimeReadError

REQUIRED_COLUMNS = ("anio", "semestre")
OUTPUT_COLUMNS = ["anio", "semestre"]
INTEGER_PATTERN = re.compile(r"^[+-]?\d+$")


def _normalize_header(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip().lower()


def _find_header(frame: pd.DataFrame) -> tuple[int, dict[str, int]] | None:
    for row_index, row in frame.iterrows():
        positions: dict[str, int] = {}
        for position, value in enumerate(row.tolist()):
            normalized = _normalize_header(value)
            if normalized in REQUIRED_COLUMNS and normalized not in positions:
                positions[normalized] = position
        if all(column in positions for column in REQUIRED_COLUMNS):
            return int(row_index), positions
    return None


def _observed_columns(frame: pd.DataFrame) -> list[str]:
    values = {
        normalized
        for value in frame.iloc[:10].to_numpy().ravel()
        if (normalized := _normalize_header(value))
    }
    return sorted(values)


def _parse_integer(value: object) -> tuple[int | None, str | None]:
    if value is None or pd.isna(value):
        return None, "valor ausente"
    if isinstance(value, bool):
        return None, "valor booleano no permitido"
    if isinstance(value, Integral):
        return int(value), None
    if isinstance(value, Real):
        numeric_value = float(value)
        if math.isfinite(numeric_value) and numeric_value.is_integer():
            return int(numeric_value), None
        return None, "valor no entero"

    text = str(value).strip()
    if INTEGER_PATTERN.fullmatch(text):
        return int(text), None
    return None, "valor no convertible a entero"


def _log_omitted_row(
    logger: logging.Logger,
    path: Path,
    sheet: str,
    source_row: int,
    anio: object,
    semestre: object,
    reason: str,
) -> None:
    logger.warning(
        "dimension_time_row_omitted path=%s sheet=%s row=%d reason=%s anio=%r semestre=%r",
        path,
        sheet,
        source_row,
        reason,
        anio,
        semestre,
        extra={
            "event": "dimension_time_row_omitted",
            "source_path": str(path),
            "sheet": sheet,
            "source_row": source_row,
            "raw_anio": anio,
            "raw_semestre": semestre,
            "reason": reason,
            "action": "omit",
        },
    )


def read_dim_tiempo(
    path: Path,
    *,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """Read and validate the canonical time dimension from an Excel workbook."""
    active_logger = logger or logging.getLogger(__name__)
    path = Path(path)

    if not path.is_file():
        raise DimensionTimeReadError(
            f"No se pudo leer la dimensión tiempo: el archivo no existe: {path}"
        )

    try:
        workbook = pd.ExcelFile(path, engine="openpyxl")
    except Exception as error:
        raise DimensionTimeReadError(
            f"No se pudo leer la dimensión tiempo desde {path}: {error}"
        ) from error

    candidates: list[tuple[str, pd.DataFrame, int, dict[str, int]]] = []
    observed: dict[str, list[str]] = {}
    for sheet in workbook.sheet_names:
        raw_frame = pd.read_excel(workbook, sheet_name=sheet, header=None, dtype=object)
        header = _find_header(raw_frame)
        observed[sheet] = _observed_columns(raw_frame)
        if header is not None:
            header_row, positions = header
            candidates.append((sheet, raw_frame, header_row, positions))

    if not candidates:
        raise DimensionTimeReadError(
            f"No se encontraron las columnas requeridas {REQUIRED_COLUMNS} en {path}. "
            f"Hojas revisadas y columnas observadas: {observed}"
        )
    if len(candidates) > 1:
        candidate_sheets = [candidate[0] for candidate in candidates]
        raise DimensionTimeReadError(
            f"Se encontraron varias hojas candidatas para la dimensión tiempo en {path}: "
            f"{candidate_sheets}. Debe existir una sola hoja con {REQUIRED_COLUMNS}."
        )

    sheet, raw_frame, header_row, positions = candidates[0]
    selected = raw_frame.iloc[
        header_row + 1 :, [positions["anio"], positions["semestre"]]
    ].copy()
    selected.columns = OUTPUT_COLUMNS

    valid_rows: list[dict[str, int]] = []
    seen_keys: set[tuple[int, int]] = set()
    for row_index, row in selected.iterrows():
        raw_anio = row["anio"]
        raw_semestre = row["semestre"]
        source_row = int(row_index) + 1

        if all(
            value is None or pd.isna(value) or str(value).strip() == "" for value in row
        ):
            _log_omitted_row(
                active_logger,
                path,
                sheet,
                source_row,
                raw_anio,
                raw_semestre,
                "fila vacía",
            )
            continue

        anio, anio_error = _parse_integer(raw_anio)
        semestre, semestre_error = _parse_integer(raw_semestre)
        reason = anio_error or semestre_error
        if reason is not None:
            _log_omitted_row(
                active_logger, path, sheet, source_row, raw_anio, raw_semestre, reason
            )
            continue
        if anio <= 0:
            _log_omitted_row(
                active_logger,
                path,
                sheet,
                source_row,
                raw_anio,
                raw_semestre,
                "anio debe ser un entero positivo",
            )
            continue
        if semestre not in (1, 2):
            _log_omitted_row(
                active_logger,
                path,
                sheet,
                source_row,
                raw_anio,
                raw_semestre,
                "semestre debe ser 1 o 2",
            )
            continue

        key = (anio, semestre)
        if key in seen_keys:
            _log_omitted_row(
                active_logger,
                path,
                sheet,
                source_row,
                raw_anio,
                raw_semestre,
                "duplicado de anio y semestre",
            )
            continue

        seen_keys.add(key)
        valid_rows.append({"anio": anio, "semestre": semestre})

    result = pd.DataFrame(valid_rows, columns=OUTPUT_COLUMNS).astype(
        {"anio": "int64", "semestre": "int64"}
    )
    if result.empty:
        active_logger.error(
            "dimension_time_empty_result path=%s sheet=%s reason=no quedaron filas válidas",
            path,
            sheet,
            extra={
                "event": "dimension_time_empty_result",
                "source_path": str(path),
                "sheet": sheet,
                "action": "return_empty_dataframe",
            },
        )
    return result
