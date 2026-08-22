# Deterministic Reporting and Case Audit

## Scope

Reporting renders a complete, already-registered DCF or comparable-company
model run. It does not discover data, calculate a new valuation, or fill in
missing inputs. Every command requires the artifact workspace explicitly.

```text
finresearch --workspace PATH report markdown CASE_ID --model-run-id RUN_ID
finresearch --workspace PATH report html CASE_ID --model-run-id RUN_ID
finresearch --workspace PATH case audit CASE_ID --as-of YYYY-MM-DD \
  --max-price-age-days N [--verify-hashes]
```

All three commands require a manifest v2 case. Migrate a valid legacy case
explicitly with `case migrate CASE_ID`; neither reporting nor audit changes a
v1 manifest implicitly.

## Report preflight and outputs

Before reading a model table, a report validates its registered bytes, exact
v2 ordered parent and input-file-hash bindings, and Parquet `DatasetContract`.
It authenticates the DCF cutoff by checking the exact registered
`dcf-inputs.toml` bytes and parsing its strict `as_of`; it authenticates a
comps cutoff from the hash-bound typed `run_as_of`, source artifact bytes,
requested metrics, target, and registers. The same resolver reconstructs every
canonical typed input, output, sensitivity, summary, and reconciliation frame
from those inputs; any published frame that differs is rejected even if its
manifest metadata and checksums were coherently changed.
Every coherent model artifact must declare that authenticated cutoff at UTC
midnight. Historic `model.comps-inputs.v1` tables remain data-valid but cannot
authenticate the CLI cutoff; report and audit reject a complete v1 comps run
with an explicit rerun requirement. A DCF run must have
exactly one each of `model.dcf-inputs`, `model.dcf-cashflows`,
`model.dcf-results`, and `model.dcf-reconciliation` linked in that order; an
optional DCF sensitivity table must also be linked to results. A comps run must
have exactly one each of `model.comps-inputs`, `model.comps-results`,
`model.comps-summary`, and `model.comps-reconciliation` with their declared
edges. A failed reconciliation, missing table, mismatched run identity, or
cross-run parent prevents publication.

The report preflight audits only the selected authenticated run and its
transitive declared inputs at the model cutoff. Thus an unrelated older,
future, partial, or legacy run cannot veto a valid selected report; a selected
legacy v1 comps run remains explicitly rejected. It writes nothing if any
selected-run preflight check fails.

Markdown and self-contained HTML are rendered from a small typed context. They
show the run id, cutoff, producing command, source ids, direct source artifact
ids where the model has them, and model artifact ids. HTML escapes all table
and metadata values, has no external assets, and uses semantic tables. Markdown
also HTML-escapes raw markup and normalizes newlines while escaping table
delimiters, backslashes, and link/image delimiters; C0, DEL, and C1 control
characters are rendered as visible `U+` escapes. A DCF sensitivity grid, when
registered, is included as a deterministic inline table.

For a DCF run whose authenticated sensitivity data contains a complete 2D grid
for every rendered scenario (at least two WACC values and two terminal-growth
values), HTML additionally includes one dependency-free inline SVG heatmap per
scenario. Each figure has an accessible title and description, keeps the
semantic sensitivity table, uses only finite authenticated frame values, and
names the exact `model.dcf-sensitivity` artifact plus its canonical `model dcf`
producing command in the caption. It has no external assets. One-dimensional
or incomplete grids retain the table without a heatmap. Markdown deliberately
keeps the portable table only.

Reports are immutable `report.markdown.v1` or `report.html.v1` artifacts under
the case `reports` role. Their identity is the canonical format, run id,
ordered model-parent ids and checksums, and report producer version. The report
artifact records those sorted parents as v2 lineage; parent-file hash records
are produced by the common publisher. Rendering has no wall-clock field: the
artifact timestamp is the model `as_of` at UTC midnight. An identical rerun
returns the existing receipt. A different byte sequence or declaration at the
same identity is an integrity error and is never overwritten.

The Markdown renderer remains `finresearch.report` producer version `1`.
HTML producer version `1` is frozen for reverse authentication of existing
Phase 3 reports; current HTML publication uses producer version `2` for the
heatmap/CSS contract. Producer version is part of the immutable identity and
the v2 filename is explicitly prefixed with `v2.`, so it selects a distinct
report path; audit rejects unknown renderer versions rather than interpreting
them as either contract.

## Case audit

`case audit` is read-only and exits nonzero when any stable issue is found. It
first invokes the existing structural case validation and deep artifact
validation, including the manifest graph, authoritative parent bindings, safe
case-relative input-file declarations whose paths exist as regular files,
registered Parquet contracts, model reconstruction,
report reconstruction, and existing cross-artifact currency checks. By default
it does not digest every unrelated artifact byte or input file. `--verify-hashes`
additionally requests that full-byte check for every registered artifact and
every declared input-file record, including Markdown and HTML reports. Report publication
always uses the full-byte mode for its selected run.

The same mode is threaded through model and report authentication. Without
`--verify-hashes`, authentication still requires safe existing regular files,
strict contracts, exact lineage declarations, and exact semantic reconstruction
of model frames and report bytes, but uses declared manifest/input hashes as
identity values rather than digesting current file bytes. With the flag, it also
compares every current model input, parent, output, and report byte digest.

The audit then applies deterministic point-in-time checks:

- facts cannot have `knowledge_date` after the cutoff;
- estimates cannot have `estimate_as_of` or `availability_at` after it;
- instrument-master observations cannot have `observed_at` after it;
- price sessions cannot be after it, and each instrument's latest valid price
  (a session on or before the cutoff) must not exceed
  `--max-price-age-days`; a future-only instrument has no valid session and is
  reported separately;
- authenticated model runs may be dated on or before the cutoff; only runs
  after the cutoff are issues; and
- DCF/comps input rows must resolve to valid evidence or assumption ids whose
  effective dates are not after the cutoff, while comps input rows must use the
  hash-bound model cutoff.

Issue codes and messages are deterministic (for example,
`price_stale`, `fact_knowledge_after_as_of`, `model_source_after_as_of`,
`report_identity_invalid`, and `checksum_mismatch`) and printed in code/message
order. Registered reports are reconstructed from their exact authenticated
model parents, then checked for their canonical declaration and exact rendered
bytes. The audit never repairs or rewrites a manifest, artifact, register, or
report.

Evidence and assumption registers use strict CSV projections with exactly the
documented ordered header and no duplicate names. Invalid UTF-8, malformed
headers, missing or extra columns, and over-wide or short rows are reported as
stable register issues; audit and report commands do not reinterpret malformed
CSV.
