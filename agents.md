# AGENTS.md

## important rules

- never run `dune clean`.
- never use the `--force` argument.
- never run dune build with `DUNE_CACHE=disabled`.
- never try to remove the dune lock file.
- never git checkout or reset any file unless explicitly requested.
- never preserve, migrate, or add compatibility for old APIs or old data formats
  unless the user explicitly requests compatibility in the current task. For
  redesigns, delete obsolete concepts and make old shapes fail loudly.
- never hide warnings and never hide unused variables by adding an underscore.
  treat warnings as errors that need a real fix.
- do not use opam, we use dune package management. invoke dune directly to build and test.

## commit guidelines

- before staging, inspect the worktree and prove which changes belong to the
  current task. Never infer ownership from thematic similarity; concurrent and
  pre-existing changes remain unstaged.
- stage only task-owned paths and hunks. Use partial staging when a file mixes
  current-task changes with other work, then review the complete staged diff
  and run `git diff --cached --check` before committing.
- follow the repository's conventional subject style:
  `<type>(<scope>): <Imperative subject>` (omit the scope only when the change
  genuinely spans the repository).
- give every non-trivial commit a self-contained body, the prior behavior and
  why it mattered; the new behavior and where the invariant or ownership now
  lives; then the evidence and any honest limitation.
- lead with causality, not a change inventory or test count. Use bullets only
  for independent semantic changes, never as a file list. For test-only
  commits, name the observable contract and justify any harness-only seam or
  non-visual assertion.
- write for future readers: do not depend on temporary plans or ledgers,
  unexplained local jargon, or chronology that is not needed to understand the
  resulting code.
- never mention build, test or other obvious, assumed checks in the commits.

## api design guidelines

- prefer solving API design problems with composition and small combinators
  before introducing wrapper types, service objects, registries, or managers.
- a type is a domain concept. Moving a type into an existing module does not
  remove the concept; it only changes where the concept is named.
- choose `type` versus `module` by the operations the concept needs. If a type
  has public constructors, accessors, validation, comparison, parsing,
  formatting, codecs, or other functions, give it its own module.
- use transparent records or variants only when callers benefit from directly
  constructing and pattern matching on the data and there are no meaningful
  invariants or operations to protect.
- do not introduce a new abstraction to group data that existing types and
  composable functions can express clearly.
- avoid exceptions; use exceptions only for impossible or programmer-local invalid construction,
  such as invalid static identifiers in provider definitions. At boundaries
  that read user input, config files, environment variables, stores, composed
  provider sets, permissions, or other runtime state, use structured
  `(value, error) result` errors.
- avoid using catch-all exception handlers (with _ ->) as these can hide unexpected
  errors and make debugging difficult.
- keep recoverable errors structured. Provide `message`/`pp` for CLI and
  diagnostics, but do not make callers or tests depend on parsing human-readable
  strings.
- prefer using effects and direct style vs monads.
- prefer loops to recursion. Intermediate float values can be optimized to be unboxed in loops,
  but not across recursion.
- do not use the Obj module, it bypasses the type system and can lead to seg faults.
- do not use the marshal module, as it can lead to unsafe behavior. Use a typed
  codec (a wire or cbor encoder and decoder, or a hand-written printer and parser) instead. If a trusted in-process boundary truly needs it, isolate it in one module and document why.
- avoid using polymorphic structural comparisons (=), (<>), (<), (>), (<=), (>=)
- do not use the Str module, as it has a problematic API. Use the `tyre` module instead for regular
  expressions. `Tyre` provides a better API, performance, type safety, and avoids global state.
- avoid using ppx in general, unless they are required for embedding some important dsl into
  the language. ppx makes ocaml harder to read and slower and more awkward to compile.
- use the `Dtoa` library for converting floats to strings. The native implementation can have
  platform-specific inconsistencies due to depending on the host machine's c libraries.
- no not use `float array`, use `floatarray` instead for consistent boxing and performance.

## test guidelines

- host-level tests SHOULD NOT be unit tests, they should be blackbox integration tests
  testing user facing behavior and contracts.
- make sure you are inside the nix shell before building and running tests.
- if on a mac environment, The tests can be run on linux with the script under `./scripts/runtest-linux.sh`.
