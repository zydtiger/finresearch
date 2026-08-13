# Case Contract v1

## Scope

The v1 contract defines the directory and manifest foundation shared by later
data schemas, imports, models, and reports. It does not define any Parquet table
schema.

Every command receives the artifact workspace explicitly:

```bash
finresearch --workspace /path/to/finresearch-artifacts case status CASE_ID
```

The CLI does not discover, configure, or infer the workspace.

## Workspace and case tree

```text
<workspace>/
└── cases/
    └── <case-id>/
        ├── manifest.toml
        ├── registers/          # optional; created when research registers exist
        ├── data/
        │   ├── raw/            # required; immutable source inputs
        │   ├── normalized/     # required; canonical source-faithful data
        │   └── derived/        # required; reproducible calculations
        ├── analysis/           # optional; user/agent-owned case-specific Python
        └── reports/            # required; Markdown and HTML deliverables
```

`case init` creates the manifest and required directories. It does not create
register files, analysis scripts, placeholder data, or reports.

## Case identifiers

A case ID is supplied by the caller and is never silently normalized. It must:

- contain 1-64 lowercase ASCII letters, digits, or hyphens;
- start and end with a letter or digit; and
- be unique below `<workspace>/cases/`.

Examples include `aapl-2026-08-11` and `btc-cycle-update`. Initialization fails
without changing an existing case when the ID already exists.

## Manifest v1

The manifest is TOML and starts with this shape:

```toml
manifest_version = 1
case_id = "aapl-2026-08-11"
title = "Apple valuation update"
status = "active"
artifacts = []

[paths]
registers = "registers"
raw = "data/raw"
normalized = "data/normalized"
derived = "data/derived"
analysis = "analysis"
reports = "reports"
```

Supported case statuses are `active`, `paused`, `completed`, and `archived`.
Manifest v1 requires all six path roles. Paths use POSIX separators, remain
relative to the case directory, and cannot contain `..`, resolve through a
symlink outside the case, or reuse another role path.

## Artifact declarations

Artifacts are optional during initialization. A later workflow can add entries
with this shape:

```toml
[[artifacts]]
id = "market.prices.daily"
kind = "prices"
schema_version = 1
path = "data/normalized/prices.parquet"
source = "provider-name"
sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
retrieved_at = "2026-08-11T03:12:45.123456Z"
row_count = 252
```

Each artifact records its own positive `schema_version`; table contracts evolve
independently. Artifact IDs and paths are unique within a case. Artifact paths
must stay below a directory declared in `[paths]`. When present, `sha256` uses
64 lowercase hexadecimal digits. Network ingestions also record an RFC 3339 UTC
`retrieved_at` timestamp and a non-negative `row_count`. These provenance fields
remain optional for artifacts that are not row-oriented provider snapshots.
`source` names the provider for raw snapshots; normalized artifacts record the
raw artifact id that produced them, making each row traceable to its input.

## Command behavior

```text
case init CASE_ID [--title TEXT]
case status CASE_ID
case validate CASE_ID
```

- `init` creates a new case and never overwrites a collision.
- `status` reports manifest validity, lifecycle status, required directories,
  and declared/present/missing artifact counts.
- `validate` returns success only when the manifest is valid, every required
  directory exists, and every declared artifact exists as a file.

Malformed TOML, unsupported manifest versions, mismatched case IDs, invalid or
escaping paths, missing required directories, and missing declared artifacts
produce actionable errors and a nonzero command exit.
