"""Code location entry point for the example pipelines.

``dagster dev -m example_pipelines.definitions`` loads every defs.yaml under
``example_pipelines.defs`` and merges the environment-backed shared resources.
"""

from __future__ import annotations

from pathlib import Path

import dagster as dg

from data_platform_common.resources import build_resources_from_env


@dg.definitions
def defs() -> dg.Definitions:
    component_defs = dg.load_from_defs_folder(path_within_project=Path(__file__))
    return dg.Definitions.merge(
        component_defs,
        dg.Definitions(resources=build_resources_from_env()),
    )
