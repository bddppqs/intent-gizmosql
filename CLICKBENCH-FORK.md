# ClickBench build of GizmoSQL v1.38.0

This repository is GizmoSQL v1.38.0 with one change: the embedded DuckDB is
pinned to https://github.com/bddppqs/intent-duckdb at tag `v1.5.5-clickbench.1`, a fork of DuckDB
v1.5.5 whose engine changes are documented in that repository's
`CLICKBENCH-FORK.md`. The server code, protocol, configuration defaults and startup flags are
upstream's.

Upstream's CI, documentation and MSI workflows are removed here because they require upstream
secrets and publish upstream artifacts (container images, Homebrew, PyPI). The
`clickbench-release` workflow builds the portable Linux `gizmosql_server` and `gizmosql_client`
inside `quay.io/pypa/manylinux_2_28_x86_64@sha256:0536c364004fa2a3c5041120b6fe35d84fc5bfe31f04c6a6304f13eac4a67b63` on a tag push and attaches `gizmosql_cli_linux_amd64.zip` and
`SHA256SUMS` to the matching GitHub release. Tag `v1.38.0-clickbench.1` is the release the
ClickBench entry `intent-gizmosql` installs.

## Evaluation and labeling

The DuckDB changes were developed and evaluated against the 43 ClickBench queries. The entry
runs the default GizmoSQL configuration with the upstream ClickBench schema and load path; it
is submitted as a derivative system, and the ClickBench maintainers decide its final name and
labels.

## License

GizmoSQL's Apache-2.0 license applies unchanged; see `LICENSE` and `NOTICE`.
