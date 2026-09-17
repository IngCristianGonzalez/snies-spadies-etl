"""Tests de calidad de la dimensión sexo y del mapeo de género SNIES.

Cubre R2, R5 y R7 de `specs/015_dimension_sexo/requirements.md`.
"""

import pandas as pd
import pytest

from etl.transform_sexo import (
    METRIC_COLUMNS,
    SIN_INFORMACION_CODE,
    SexoContractError,
    map_genero_codes,
    normalize_sexo_dimension,
    sexo_dimension_metrics,
)


def test_sexo_duplicate_code_with_different_label_raises_error() -> None:
    frame = pd.DataFrame({"codigo": [1, 1], "descripcion": ["MASCULINO", "FEMENINO"]})

    with pytest.raises(SexoContractError) as excinfo:
        normalize_sexo_dimension(frame)

    assert "codigo 1" in str(excinfo.value)
    assert excinfo.value.conflicts


def test_sexo_duplicate_code_same_label_is_collapsed() -> None:
    frame = pd.DataFrame(
        {"codigo": [1, 1], "descripcion": ["MASCULINO", " masculino "]}
    )

    result = normalize_sexo_dimension(frame)

    assert len(result[result["codigo"] == 1]) == 1


def test_sexo_null_or_non_numeric_code_raises_error() -> None:
    frame = pd.DataFrame(
        {"codigo": [1, None, "x"], "descripcion": ["MASCULINO", "FEMENINO", "TRANS"]}
    )

    with pytest.raises(SexoContractError):
        normalize_sexo_dimension(frame)


def test_sexo_null_description_raises_error() -> None:
    frame = pd.DataFrame({"codigo": [1, 2], "descripcion": ["MASCULINO", None]})

    with pytest.raises(SexoContractError):
        normalize_sexo_dimension(frame)


def test_sexo_missing_columns_raises_error() -> None:
    frame = pd.DataFrame({"codigo": [1, 2]})

    with pytest.raises(SexoContractError):
        normalize_sexo_dimension(frame)


def test_snies_genero_null_zero_maps_to_sin_informacion() -> None:
    series = pd.Series([1, 0, None, "3", "x"])

    mapped, _ = map_genero_codes(series)

    assert mapped.tolist() == [
        1,
        SIN_INFORMACION_CODE,
        SIN_INFORMACION_CODE,
        3,
        SIN_INFORMACION_CODE,
    ]


def test_snies_genero_mapping_counts_affected_rows() -> None:
    series = pd.Series([1, 0, None, "x", 2, 4])

    mapped, affected = map_genero_codes(series)

    assert affected == 3
    assert mapped.iloc[0] == 1
    assert mapped.iloc[4] == 2


def test_sexo_metrics_report_expected_columns() -> None:
    metrics = sexo_dimension_metrics(
        source="dim_sexo.xlsx", input_rows=4, output_rows=5
    )

    assert list(metrics.columns) == list(METRIC_COLUMNS)
    assert metrics.iloc[0]["input_rows"] == 4
    assert metrics.iloc[0]["output_rows"] == 5
    assert metrics.iloc[0]["status"] == "ok"
