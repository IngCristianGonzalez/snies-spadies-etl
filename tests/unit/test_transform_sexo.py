"""Tests unitarios de la transformación de la dimensión sexo.

Cubre R1, R3, R4 y R6 de `specs/015_dimension_sexo/requirements.md`.
"""

import inspect

import pandas as pd

from etl.transform_sexo import (
    SIN_INFORMACION_CODE,
    SIN_INFORMACION_LABEL,
    normalize_sexo_dimension,
)


def test_sexo_dimension_input_contract_accepted() -> None:
    frame = pd.DataFrame({"codigo": [1, 2], "descripcion": ["masculino", "femenino"]})

    result = normalize_sexo_dimension(frame)

    assert list(result.columns) == ["codigo", "descripcion"]


def test_sexo_dimension_returns_canonical_frame() -> None:
    frame = pd.DataFrame(
        {
            "codigo": ["1", "2", "4"],
            "descripcion": ["  masculino ", "FÉMENINO", "no binario"],
        }
    )

    result = normalize_sexo_dimension(frame)

    assert result["codigo"].dtype == "int64"
    assert result["codigo"].tolist() == [1, 2, 4, SIN_INFORMACION_CODE]
    assert result.loc[result["codigo"] == 1, "descripcion"].iloc[0] == "MASCULINO"


def test_sexo_dimension_includes_sin_informacion_code_9() -> None:
    frame = pd.DataFrame(
        {
            "codigo": [1, 2, 3, 4],
            "descripcion": ["MASCULINO", "FEMENINO", "TRANS", "NO BINARIO"],
        }
    )

    result = normalize_sexo_dimension(frame)

    row = result[result["codigo"] == SIN_INFORMACION_CODE]
    assert len(row) == 1
    assert row.iloc[0]["descripcion"] == SIN_INFORMACION_LABEL


def test_sexo_transform_has_no_db_dependency() -> None:
    import etl.transform_sexo as module

    source = inspect.getsource(module)

    assert "config.database" not in source
    assert "create_engine" not in source
