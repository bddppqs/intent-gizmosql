# ClickBench build of GizmoSQL v1.38.0

This repository is GizmoSQL v1.38.0 with the embedded DuckDB pinned to https://github.com/bddppqs/intent-duckdb at tag
`v1.5.5-intent.2` (see that repository's `CLICKBENCH-FORK.md`). `v1.38.0-clickbench.1` was the first release;
`v1.38.0-intent.2` is the one the ClickBench entry `intent-gizmosql` installs.

The server and client changes are listed in `CHANGELOG.md`: a statement is prepared once, statements run on persistent
executor threads, the client sends each query as one request, and repeated read-only statements reuse a bounded cache of
optimized plans (`kPlanCacheEntries`, 256 plans); no result is cached. For a statement with a cached plan, the
hash-aggregate state is released on a background thread after its result is sent (`kMaxPendingReaps`: at most 4 such
releases pending). The protocol and startup flags are upstream's.

Upstream's CI, documentation and MSI workflows need upstream secrets, so they are removed. The portable Linux binaries
are built inside manylinux_2_28 images (glibc 2.28 baseline) with the `clickbench-release` workflow's recipe: amd64 in
`quay.io/pypa/manylinux_2_28_x86_64@sha256:0536c364004fa2a3c5041120b6fe35d84fc5bfe31f04c6a6304f13eac4a67b63`, arm64 in `quay.io/pypa/manylinux_2_28_aarch64` on an aarch64 machine; both are attached to the
release with SHA-256 sums. The changes were developed and evaluated against the 43 ClickBench queries.

## License

GizmoSQL's Apache-2.0 license applies unchanged; see `LICENSE` and `NOTICE`.
