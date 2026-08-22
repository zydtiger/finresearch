# Case Contract v2

## Scope

The v2 contract defines the directory and manifest foundation shared by later
data schemas, imports, models, and reports. It does not define any Parquet table
schema. Readers continue to validate existing v1 manifests unchanged; new cases
are initialized as v2.

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

## Manifest versions

Manifest version is a strict schema boundary. A v1 manifest accepts only v1
artifact fields, and a v2 manifest accepts only v2 artifact fields. In
particular, a v1 manifest does not silently accept v2 producer or lineage
fields, and v2 does not use v1's ambiguous `source` field.

### Manifest v2

The manifest is TOML and starts with this shape:

```toml
manifest_version = 2
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
Both supported manifest versions require all six path roles. Paths use POSIX
separators, remain relative to the case directory, and cannot contain `..`,
resolve through a symlink outside the case, or reuse another role path.

### Manifest v1 compatibility

Existing v1 manifests remain readable and valid under their original schema.
They are never upgraded implicitly. Use the explicit migration command to
write a v2 manifest when the case is ready for multi-parent lineage and
deterministic producer declarations.

## Artifact declarations

Artifacts are optional during initialization. A v1 artifact has this legacy
shape:

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
must stay below a directory declared in `[paths]`. In v1, when present,
`sha256` uses 64 lowercase hexadecimal digits. Network ingestions also record
an RFC 3339 UTC `retrieved_at` timestamp and a non-negative `row_count`. These
provenance fields remain optional for artifacts that are not row-oriented
provider snapshots.
`source` names the provider for raw snapshots; normalized artifacts record the
raw artifact id that produced them, making each row traceable to its input.
It is a v1-only field.

A v2 artifact has this strict shape. Its own `sha256` is required for every
artifact; retrieval time and row count remain optional where they do not apply:

```toml
[[artifacts]]
id = "normalized.prices.aapl.6b6f"
kind = "normalized.daily-prices"
schema_version = 1
path = "data/normalized/normalized.daily-prices/aapl/6b6f.parquet"
input_artifact_ids = ["raw.yfinance.daily-prices.aapl.snapshot"]
producer = "finresearch.data.normalize"
producer_version = "2"
parameters_sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
input_file_hashes = [
  { name = "artifact.raw.yfinance.daily-prices.aapl.snapshot", path = "data/raw/yfinance/daily-prices/aapl/snapshot.parquet", sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" },
]
sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
retrieved_at = "2026-08-11T03:12:45.123456Z"
row_count = 252
```

`input_artifact_ids` is the authoritative, ordered parent list. Every parent
must be declared somewhere in the manifest; declaration order does not matter.
Parent ids cannot be duplicated, self-referential, or part of a cycle. Artifact
ids and output paths remain unique, and output paths must stay below a declared
path role.

`producer`, `producer_version`, and `parameters_sha256` identify the exact
deterministic transformation. `parameters_sha256` is a SHA-256 digest of the
canonical producer parameters. `input_file_hashes` is an ordered array of
inline tables with exactly `name`, `path`, and `sha256`: names use the artifact
name syntax, paths are non-empty POSIX case-relative paths that cannot escape
the case, and hashes are 64 lowercase hexadecimal digits. Duplicate input-hash
names and paths are rejected. Every parent id must have exactly one canonical
record named `artifact.<parent-id>` with that parent's declared path and, when
the parent has a declared checksum, the same checksum. Other inputs such as a
register or parameter file may use non-`artifact.` names. The records make the
input bytes to models and reports auditable even when their inputs are not all
tabular artifacts. `data validate` verifies each v2 input-file record against
the current bytes; a missing input file or checksum mismatch is an actionable
validation failure for every artifact type.

V2 publication is non-destructive. Repeating a publication with the same
identity, content, and declaration returns its existing receipt. Reusing that
identity with different bytes or metadata is an integrity error; no file or
manifest entry is overwritten.

## Command behavior

```text
case init CASE_ID [--title TEXT]
case migrate CASE_ID
case status CASE_ID
case validate CASE_ID
```

- `init` creates a new case and never overwrites a collision.
- `migrate` explicitly upgrades a valid v1 manifest to v2. A legacy `source`
  becomes a parent edge only for an artifact under the `normalized`, `derived`,
  or `reports` path role when it names a declared artifact; a raw provider name
  is never inferred as lineage, even if it collides with an artifact id. Before
  replacement, migration reads every declared artifact: missing bytes or a
  mismatched legacy checksum aborts without changing the manifest, while a
  missing legacy checksum is populated from the calculated bytes. It atomically
  replaces only `manifest.toml` content and never rewrites declared artifact
  bytes. It may create or reuse the internal `.finresearch.lock` file while
  serializing the operation. Migration is idempotent when the case is already
  v2.
- `status` reports manifest validity, lifecycle status, required directories,
  and declared/present/missing artifact counts.
- `validate` returns success only when the manifest is valid, every required
  directory exists, and every declared artifact exists as a file.

Malformed TOML, unsupported manifest versions, mismatched case IDs, invalid or
escaping paths, missing required directories, and missing declared artifacts
produce actionable errors and a nonzero command exit.
