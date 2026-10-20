"""Empty package whose directory holds component defs.yaml files."""

# The defs module is discovered from [tool.dg] project.root_module = "example_pipelines".
# load_from_defs_folder() walks this package for defs.yaml documents and builds
# one CompositeYamlComponent per file (each file may hold multiple documents).
