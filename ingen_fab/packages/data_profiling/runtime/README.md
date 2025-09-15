# Runtime Code

This directory contains all the runtime code that gets deployed to Microsoft Fabric notebooks.

## Structure

- **core/** - Core domain models, interfaces, and enums
  - **models/** - Data models (profiles, metadata, relationships, statistics)
  - **interfaces/** - Abstract interfaces for profiling and persistence
  - **enums/** - Enumerations for profile types, semantic types, etc.

- **profilers/** - Profiling implementations
  - **tiered/** - Tiered profiling system with 4 scan levels
  - **basic/** - Basic profiling implementation

- **analyzers/** - Analysis components
  - Pattern detection, relationship discovery, quality scoring

- **persistence/** - Storage implementations
  - Lakehouse and warehouse persistence adapters

- **factories/** - Factory patterns for creating profilers and analyzers

- **utils/** - Shared utilities
  - Spark helpers, type checking, JSON conversion

- **config/** - Runtime configuration

## Usage in Notebooks

Generated notebooks import from this runtime package:

```python
from ingen_fab.packages.data_profiling.runtime.core.models import DatasetProfile
from ingen_fab.packages.data_profiling.runtime.core.enums import ProfileType
from ingen_fab.packages.data_profiling.runtime.profilers.tiered import TieredProfiler
```

## Important Notes

- This code runs in the Fabric environment
- Must be self-contained with minimal external dependencies
- All code here is embedded/imported into generated notebooks
- Do NOT include compilation-time code here