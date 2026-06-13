---
name: ocaml-cram
description: "Cram (shell-session) integration tests for OCaml packages. Use when adding a test that drives a built executable and diffs its stdout against a committed transcript, when a cram test can't find its executable, when output is non-deterministic, or when shared helpers should be factored across several cram tests."
license: ISC
metadata:
  copyright: "Copyright (c) 2026 Thomas Gazagnaire <thomas@gazagnaire.org>"
---

# Cram Tests

A cram test is a shell session: commands plus expected output. Dune runs it in a
sandbox, diffs stdout against the transcript, and offers promotion on mismatch.

Use cram only when the thing being verified is **what the user sees** when they
run the command: exact stdout/stderr, exit code, error messages. For a pure
library API use alcotest; for golden binary bytes use `Alcotest.check bytes`
against a committed fixture; for property testing use alcobar in `fuzz/`;
for unit assertions `test_<mod>.ml` scales better than a large `run.t`.

## A cram test is a golden UX transcript

Make the transcript tell a real story a human can follow without knowing the
codebase:

- **Demonstrate a workflow**, not an assertion on incidental output. A progress
  demo prints the visible state after each step (`[ 33%] 1/3 foo.ml`), not an
  internal flag.
- **Label the steps** (`initial:`, `after foo.ml:`, `after finish:`) so the
  reader can orient.
- **If the output is hard to validate, fix the driver, not the transcript.**
  When `Console.Display` emitted empty strings because the driver captured the
  wrong formatter, the fix was rewriting the driver to use the functional core
  (`Progress.render`), not accepting empty lines in `run.t`.
- **Never weaken an assertion to make a test pass.** Unstable output gets
  stabilized in the driver (trim whitespace, seed randomness, pin timestamps),
  not globbed away.

A good cram test breaks cleanly when the UX regresses. If a diff is meaningful
("we used to show a progress bar, now a blank line"), it is doing its job; if it
is noise ("width padding changed"), tighten the driver.

## Layout

All cram tests in a package live under `test/cram/`, one directory per test.

```txt
<package>/test/
├── dune                   # (test ...) for the alcotest runner, nothing else
├── test.ml
├── test_<mod>.ml(i)
└── cram/
    ├── dune               # one (cram ...) for the whole subtree
    ├── helpers.sh         # auto-sourced before every test
    ├── helpers/
    │   ├── dune           # (executable ...) stanzas for driver exes
    │   └── <driver>.ml
    ├── cli.t/run.t
    └── errors.t/run.t
```

**Always the directory form** (`cli.t/run.t`), never the single-file form
(`cli.t`): the file form cannot carry fixtures or share a driver, and
standardizing costs nothing.

### `test/cram/dune`

```dune
(cram
 (applies_to :whole_subtree)
 (deps helpers/demo.exe)
 (setup_scripts helpers.sh))
```

`(applies_to :whole_subtree)` covers every `.t/` directory below.
`(deps helpers/demo.exe)` builds the helper and copies it into each sandbox at
`../helpers/demo.exe` relative to the test's cwd. `(setup_scripts helpers.sh)`
(dune 3.21+) sources `helpers.sh` before each test, so its functions, exports,
and PATH changes are visible in every `run.t`.

### `test/cram/helpers.sh`

```sh
#!/bin/sh
# Sourced before every cram test under test/cram/*.t/.

# Make driver exes callable by name, no path prefix.
export PATH="$PWD/../helpers:$PATH"

# scrub: normalize volatile output to stable placeholders. Use as `./tool | scrub`.
scrub() {
  sed \
    -e "s|$PWD|\$PWD|g" \
    -e "s|$HOME|\$HOME|g" \
    -e "s|/tmp/[a-zA-Z0-9_./-]*|\$TMP|g"
}
```

`$PWD` when this is sourced is the sandbox copy of `<name>.t/`, so
`$PWD/../helpers` resolves to where dune placed the driver.

### `test/cram/helpers/dune`

```dune
(executable
 (name demo)
 (libraries <your-lib> fmt))
```

Sources live alongside. Add a second driver by adding another `(executable ...)`
here and extending `(deps ...)` in `test/cram/dune`.

### `test/cram/cli.t/run.t`

```cram
EWAH command-line demo
=======================

Empty bitmap serializes to a minimal header + one empty RLW (20 bytes):

  $ demo.exe empty
  cardinal=0 length=0 serialized=20
```

The driver is invoked by name thanks to the `PATH` export, no `./` or
`../helpers/` prefix.

### Testing a public CLI

If the package under test *is* a public CLI (`(public_name <tool>)` in
`bin/dune`), skip `helpers/` and depend on the installed binary. This fits
`merlint`, `monopam`, `precommit`, `publicsuffix`, `sqlite`.

```dune
(cram
 (applies_to :whole_subtree)
 (deps %{bin:<tool>}))
```

## Non-determinism: dune cram has no glob or regex

Dune matches expected output line-by-line by **exact equality**. There is no
`(glob)` suffix, no `(re)`, no wildcards; those are Mercurial-cram features
dune does not implement. Writing `done ??????? (glob)` fails, because dune
compares it against the literal string `done ??????? (glob)`.

For timestamps, random IDs, hashes, or tempdir paths, pipe through a filter:

```cram
  $ ./tool --commit 2>&1 | sed 's/done [0-9a-f]*/done HASH/'
  done HASH
```

Better, keep the filters in `helpers.sh` and chain them:

```sh
scrub_hash() { sed 's/[0-9a-f]\{7,40\}/HASH/g'; }
scrub_time() { sed 's|[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}T[0-9:]*|TIME|g'; }
scrub_tmp()  { sed "s|/tmp/[A-Za-z0-9._-]*|\$TMP|g"; }
```

```cram
  $ ./tool | scrub_hash | scrub_time
  Commit HASH at TIME
```

Prefer filtering in the test over patching the driver, unless the driver is
what produces the noise, in which case fix it there (pin a seed, pass a fixed
timestamp, trim trailing whitespace).

## Gotchas

- **No heredocs in `run.t`.** They hide the fixture inside the transcript.
  Commit the fixture as a file in the `.t/` directory (dune copies it into the
  sandbox), or build it into the driver.
- **`demo.exe`, not `demo`.** Dune executables carry the `.exe` suffix even on
  Unix, and `PATH` won't find `demo`. Invoke with the suffix, or alias in
  `helpers.sh`: `demo() { demo.exe "$@"; }`.
- **Watch `Fmt` wrapping.** Cram is whitespace-sensitive; `Fmt` boxes wrap at
  column 80 and `Fmt.list ~sep:sp` introduces trailing separators. For
  cram-checked output use `print_endline` or `String.concat`.
- **Don't pull sources into `test/` with `copy_files`.** The executable stanza
  belongs in `helpers/dune`.
- **Don't put an executable stanza in `test/dune`.** That file is for the
  alcotest runner and nothing else.
- **No nested `dune-project`.** Dune forbids it. The dune files under
  `test/cram/` inherit the root project context.

## Debugging a broken cram

Probe the sandbox by temporarily replacing `run.t`:

```cram
  $ pwd
  $ ls -la
  $ ls ..
  $ echo "PATH=$PATH"
  $ which demo.exe
```

Run `dune runtest --force`; the promotion diff shows the sandbox. Common causes:

| Symptom | Cause |
|---------|-------|
| `demo.exe: command not found` | The `PATH` export didn't run, check `(setup_scripts helpers.sh)` is in `test/cram/dune` |
| `../helpers` missing from the sandbox | `(deps helpers/demo.exe)` missing, or `helpers/dune` doesn't build it |
| Sandbox empty | `(cram ...)` is in `test/dune` instead of `test/cram/dune` |
