"""Contract loading and safe dotted import tests."""

from __future__ import annotations

from pathlib import Path

import pytest

import data_contracts
from data_platform_common.contracts import (
    ContractRegistry,
    load_contract_file,
    resolve_contract_source,
)
from data_platform_common.errors import (
    ContractError,
    TransformationSignatureError,
    UntrustedImportError,
)
from data_platform_common.imports import (
    build_transformation,
    derive_project_prefixes,
    is_trusted,
    resolve_dotted,
    trusted_prefixes_from_env,
)

PREFIXES = ("data_platform_common", "data_contracts", "example_pipelines", "transform_helpers")


def test_load_contract_from_path() -> None:
    contract = load_contract_file(data_contracts.get_example_silver_contract_path())
    assert contract.name == "example_entity"
    assert contract.primary_key == ["entity_id"]


def test_resolve_contract_from_dotted_callable() -> None:
    contract = resolve_contract_source(
        "data_contracts.get_example_gold_contract_path", prefixes=PREFIXES
    )
    assert contract.name == "example_entity_gold"


def test_resolve_contract_from_inline_mapping() -> None:
    contract = resolve_contract_source(
        {"name": "inline", "columns": [{"name": "id", "type": "string", "primary_key": True}]},
        prefixes=PREFIXES,
    )
    assert contract.name == "inline"


def test_untrusted_dotted_import_rejected() -> None:
    with pytest.raises(UntrustedImportError):
        resolve_dotted("os.path.join", prefixes=("data_platform_common",))
    assert not is_trusted("os.path.join", ("data_platform_common",))
    assert is_trusted(
        "data_platform_common.transformations.json_to_dataframe", ("data_platform_common",)
    )


def test_untrusted_contract_source_is_rejected() -> None:
    with pytest.raises(UntrustedImportError):
        resolve_contract_source("os.getcwd", prefixes=("data_platform_common",))


def test_registry_resolves_and_caches() -> None:
    registry = ContractRegistry(prefixes=PREFIXES)
    first = registry.resolve("data_contracts.get_example_silver_contract_path")
    second = registry.resolve("data_contracts.get_example_silver_contract_path")
    assert first is second
    assert registry.get("example_entity") is first
    assert "example_entity" in registry
    assert registry.names() == ["example_entity"]


def test_registry_missing_contract_error() -> None:
    registry = ContractRegistry(prefixes=PREFIXES)
    with pytest.raises(ContractError):
        registry.get("does_not_exist")


def test_transformation_with_only_dataframe_is_valid() -> None:
    transformation = build_transformation(
        "data_platform_common.transformations.json_to_dataframe", prefixes=PREFIXES
    )
    assert transformation.accepted_context == ()


def test_transformation_with_contract_keyword_is_valid() -> None:
    transformation = build_transformation(
        "data_platform_common.transformations.drop_columns_not_in_contract", prefixes=PREFIXES
    )
    assert "contract" in transformation.accepted_context


def test_transformation_requires_first_positional_parameter() -> None:
    with pytest.raises(TransformationSignatureError):
        build_transformation("transform_helpers.no_arguments", prefixes=PREFIXES)


def test_transformation_rejects_unknown_keyword() -> None:
    with pytest.raises(TransformationSignatureError):
        build_transformation("transform_helpers.unsupported_keyword", prefixes=PREFIXES)


def test_transformation_accepts_variadic_keywords() -> None:
    transformation = build_transformation("transform_helpers.variadic", prefixes=PREFIXES)
    assert "contract" in transformation.accepted_context


def test_transformation_rejects_positional_extras() -> None:
    with pytest.raises(TransformationSignatureError):
        build_transformation("transform_helpers.positional_extra", prefixes=PREFIXES)
    with pytest.raises(TransformationSignatureError):
        build_transformation("transform_helpers.variadic_positional", prefixes=PREFIXES)


def test_transformation_rejects_non_callable() -> None:
    with pytest.raises(TransformationSignatureError):
        build_transformation("transform_helpers.NOT_CALLABLE", prefixes=PREFIXES)


def test_env_prefixes_parse() -> None:
    env = {"DATA_PLATFORM_TRUSTED_PREFIXES": "acme, acme.sub, ,other"}
    assert trusted_prefixes_from_env(env) == ("acme", "acme.sub", "other")


def test_derive_project_prefixes_from_example_project() -> None:
    defs_path = Path(data_contracts.__file__).parents[1] / "example_pipelines" / "defs"
    prefixes = derive_project_prefixes(defs_path)
    assert "example_pipelines" in prefixes
    assert "data_contracts" in prefixes
