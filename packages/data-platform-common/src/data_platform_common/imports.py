"""Safe dotted import resolution restricted to trusted package prefixes."""

from __future__ import annotations

import importlib
import inspect
import os
import tomllib
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from data_platform_common.errors import TransformationSignatureError, UntrustedImportError

DEFAULT_TRUSTED_PREFIXES: tuple[str, ...] = ("data_platform_common",)
ENV_TRUSTED_PREFIXES = "DATA_PLATFORM_TRUSTED_PREFIXES"

# Keyword arguments a transformation may declare to receive execution context.
TRANSFORMATION_CONTEXT_KEYS: frozenset[str] = frozenset(
    {"contract", "partition", "pipeline_name", "spark", "params", "layer", "metadata"}
)


def _parse_prefixes(raw: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in raw.split(",") if part.strip())


def trusted_prefixes_from_env(env: Mapping[str, str] | None = None) -> tuple[str, ...]:
    """Read extra trusted prefixes from the environment.

    ``DATA_PLATFORM_TRUSTED_PREFIXES`` accepts a comma-separated list of package
    prefixes. Invalid entries are rejected later, when an import is attempted.
    """

    source = os.environ if env is None else env
    raw = source.get(ENV_TRUSTED_PREFIXES, "")
    return _parse_prefixes(raw)


def derive_project_prefixes(defs_path: Path) -> tuple[str, ...]:
    """Derive trusted prefixes from the project that owns a defs file.

    Walks up from ``defs_path`` to the nearest ``dg.toml`` or ``pyproject.toml``
    and collects:

    - the project's declared ``root_module``,
    - any explicit ``trusted_prefixes`` list (useful for sibling packages such as
      a separate ``data_contracts`` distribution), and
    - for source checkouts, the importable top-level packages under ``src/``.

    This keeps the trusted set local to the project being loaded rather than
    opening up arbitrary imports, and it works identically for editable
    checkouts and installed wheels.
    """

    current = defs_path if defs_path.is_dir() else defs_path.parent
    config_path: Path | None = None
    for candidate in (current, *current.parents):
        for name in ("dg.toml", "pyproject.toml"):
            if (candidate / name).exists():
                config_path = candidate / name
                break
        if config_path is not None:
            break
    if config_path is None:
        return ()

    try:
        config = tomllib.loads(config_path.read_text())
    except (OSError, tomllib.TOMLDecodeError):  # pragma: no cover - defensive
        config = {}

    node = (
        config.get("project", {})
        if config_path.name == "dg.toml"
        else config.get("tool", {}).get("dg", {}).get("project", {})
    )

    prefixes: set[str] = set()
    project_root = config_path.parent
    src_dir = project_root / "src"
    if src_dir.is_dir():
        for entry in src_dir.iterdir():
            if entry.is_dir() and (entry / "__init__.py").exists():
                prefixes.add(entry.name)
        for module in src_dir.glob("*.py"):
            if module.name != "__init__.py":
                prefixes.add(module.stem)

    root_module = node.get("root_module")
    if root_module:
        prefixes.add(str(root_module))
    for prefix in node.get("trusted_prefixes", []) or []:
        prefixes.add(str(prefix))

    if not prefixes:
        prefixes.add(project_root.name.replace("-", "_"))
    return tuple(sorted(prefixes))


def is_trusted(dotted: str, prefixes: Sequence[str]) -> bool:
    return any(dotted == prefix or dotted.startswith(f"{prefix}.") for prefix in prefixes)


def resolve_dotted(dotted: str, *, prefixes: Sequence[str]) -> Any:
    """Import and return the object named by ``dotted`` if it is trusted."""

    if not dotted or "." not in dotted:
        raise UntrustedImportError(
            f"'{dotted}' is not a dotted import path (expected 'package.module.object')"
        )
    if not is_trusted(dotted, prefixes):
        allowed = ", ".join(prefixes) or "<none>"
        raise UntrustedImportError(
            f"Import '{dotted}' is not within the trusted prefixes ({allowed}). "
            f"Extend {ENV_TRUSTED_PREFIXES} or move the code into a trusted package."
        )
    module_name, _, attribute = dotted.rpartition(".")
    try:
        module = importlib.import_module(module_name)
    except ImportError as error:
        raise UntrustedImportError(f"Could not import '{module_name}': {error}") from error
    try:
        return getattr(module, attribute)
    except AttributeError as error:
        raise UntrustedImportError(
            f"Module '{module_name}' has no attribute '{attribute}'"
        ) from error


@dataclass(frozen=True)
class Transformation:
    """A resolved, signature-validated transformation callable."""

    dotted_path: str
    fn: Callable[..., Any]
    accepted_context: tuple[str, ...] = field(default_factory=tuple)

    def apply(self, df: Any, **context: Any) -> Any:
        kwargs = {key: value for key, value in context.items() if key in self.accepted_context}
        return self.fn(df, **kwargs)


def build_transformation(
    dotted: str,
    *,
    prefixes: Sequence[str],
    allowed_context: Iterable[str] = TRANSFORMATION_CONTEXT_KEYS,
) -> Transformation:
    """Resolve and validate a transformation callable at definition-load time."""

    fn = resolve_dotted(dotted, prefixes=prefixes)
    if not callable(fn):
        raise TransformationSignatureError(f"'{dotted}' did not resolve to a callable")

    allowed = set(allowed_context)
    signature = inspect.signature(fn)
    parameters = list(signature.parameters.values())
    if not parameters:
        raise TransformationSignatureError(
            f"Transformation '{dotted}' must accept the input DataFrame as its first argument"
        )
    first = parameters[0]
    if first.kind not in (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    ):
        raise TransformationSignatureError(
            f"Transformation '{dotted}' must accept the DataFrame positionally; "
            f"first parameter '{first.name}' is {first.kind.description}"
        )

    accepted: list[str] = []
    for parameter in parameters[1:]:
        if parameter.kind is inspect.Parameter.VAR_KEYWORD:
            accepted = sorted(allowed)
            break
        if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            raise TransformationSignatureError(
                f"Transformation '{dotted}' uses *{parameter.name}; only keyword context "
                "is supported"
            )
        if parameter.name not in allowed:
            raise TransformationSignatureError(
                f"Transformation '{dotted}' declares unsupported parameter '{parameter.name}'. "
                f"Allowed context parameters: {sorted(allowed)}"
            )
        if (
            parameter.default is inspect.Parameter.empty
            or parameter.kind is inspect.Parameter.KEYWORD_ONLY
        ):
            accepted.append(parameter.name)
        else:
            accepted.append(parameter.name)

    return Transformation(dotted_path=dotted, fn=fn, accepted_context=tuple(sorted(set(accepted))))


def build_transformations(
    dotted_paths: Sequence[str],
    *,
    prefixes: Sequence[str],
    allowed_context: Iterable[str] = TRANSFORMATION_CONTEXT_KEYS,
) -> list[Transformation]:
    return [
        build_transformation(path, prefixes=prefixes, allowed_context=allowed_context)
        for path in dotted_paths
    ]


def build_trusted_prefixes(
    context_path: str | Path | None, extra: Sequence[str] = ()
) -> tuple[str, ...]:
    """Assemble the trusted import prefixes for a component load context.

    Combines the shared library prefix, the owning project's declared packages
    (from ``dg.toml``/``pyproject.toml`` and ``src/`` discovery), environment
    overrides, and per-component extras.
    """

    prefixes: set[str] = set(DEFAULT_TRUSTED_PREFIXES)
    if context_path is not None:
        prefixes.update(derive_project_prefixes(Path(context_path)))
    prefixes.update(trusted_prefixes_from_env())
    prefixes.update(extra)
    return tuple(sorted(prefixes))
