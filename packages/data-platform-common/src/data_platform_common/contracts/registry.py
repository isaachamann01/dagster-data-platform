"""In-memory registry of data contracts with reference resolution."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from pathlib import Path

from data_platform_common.contracts.loader import load_contracts_dir, resolve_contract_source
from data_platform_common.contracts.models import DataContract, ForeignKey, ForeignKeyRef
from data_platform_common.errors import ContractNotFoundError


class ContractRegistry:
    """Resolves contracts by name or trusted dotted source and caches the results."""

    def __init__(
        self, contracts: Iterable[DataContract] = (), *, prefixes: Sequence[str] = ()
    ) -> None:
        self._contracts: dict[str, DataContract] = {}
        self._sources: dict[str, DataContract] = {}
        self._prefixes = tuple(prefixes)
        for contract in contracts:
            self.add(contract)

    def add(self, contract: DataContract) -> DataContract:
        existing = self._contracts.get(contract.name)
        # Loading the same contract twice through different sources is fine as
        # long as the content matches; differing definitions are an error.
        if (
            existing is not None
            and existing is not contract
            and existing.model_dump() != contract.model_dump()
        ):
            raise ContractNotFoundError(
                f"Contract '{contract.name}' was registered with conflicting definitions"
            )
        self._contracts[contract.name] = contract
        return contract

    def add_directory(self, directory: Path) -> None:
        for contract in load_contracts_dir(directory).values():
            self.add(contract)

    def get(self, name: str) -> DataContract:
        try:
            return self._contracts[name]
        except KeyError as error:
            raise ContractNotFoundError(
                f"Contract '{name}' is not registered (known: {sorted(self._contracts)})"
            ) from error

    def try_get(self, name: str) -> DataContract | None:
        return self._contracts.get(name)

    def resolve(self, source: str | Path) -> DataContract:
        """Resolve a defs.yaml contract reference and register the result."""

        key = str(source)
        if key in self._sources:
            return self._sources[key]
        if isinstance(source, str) and source in self._contracts:
            return self._contracts[source]
        contract = resolve_contract_source(source, prefixes=self._prefixes)
        self.add(contract)
        self._sources[key] = contract
        return contract

    def resolve_reference(self, reference: ForeignKeyRef) -> tuple[DataContract, list[str]]:
        """Resolve an FK reference to its contract and referenced columns."""

        parent = self.get(reference.contract)
        columns = reference.columns or parent.primary_key_columns
        if not columns:
            raise ContractNotFoundError(
                f"Foreign key reference to '{reference.contract}' has no columns and "
                "the parent contract has no primary key"
            )
        for column in columns:
            if parent.get_column(column) is None:
                raise ContractNotFoundError(
                    f"Foreign key reference column '{column}' is not defined in "
                    f"contract '{parent.name}'"
                )
        return parent, list(columns)

    def resolve_foreign_key(self, foreign_key: ForeignKey) -> tuple[DataContract, list[str]]:
        return self.resolve_reference(foreign_key.references)

    def all(self) -> list[DataContract]:
        return sorted(self._contracts.values(), key=lambda contract: contract.name)

    def names(self) -> list[str]:
        return sorted(self._contracts)

    def __contains__(self, name: object) -> bool:
        return name in self._contracts

    def __len__(self) -> int:
        return len(self._contracts)
