---
name: ocaml-project-setup
description: Standards for OCaml project scaffolding and metadata files. Use when initializing a new OCaml library or executable project, preparing for an opam release, setting up CI, adding missing .mli/.ocamlformat files, or reviewing project structure. Triggers on phrases like "new OCaml project", "set up this project", "dune-project", "prepare for release", "add CI", or "project structure".
license: ISC
metadata:
  copyright: "Copyright (c) 2026 Invariant Systems. All rights reserved."
---

# OCaml Project Setup

Scaffolding exists to make the code trustworthy: interfaces are explicit,
formatting is mechanical, licensing is unambiguous, and CI proves the build.
Set it up once, correctly, and it disappears from view.

The way that we prescribe ocaml projects dependencies to be handled is with
Nix for the system/dev-environment tools in a flake.nix, and ocaml packages
handled by dune. No external package manager should be used to install anything,
and any system tools that become depended upon by any workflow in the project should be added to the flake.nix.

## 1. Initialization workflow

When creating or completing a project's scaffolding:

1. Check what already exists — never overwrite a working `dune-project`,
   `.ocamlformat`, or CI config wholesale; fill gaps.
2. Determine author and license: `git config user.name` / `user.email`, an
   existing `LICENSE*` file, or ask.
3. Create the required files (section 2), pinning versions from the installed
   tools, not from memory (section 3).
4. Add an `.mli` for every library module (section 4).
5. Set up the test suite — load `ocaml-testing`.
6. Add CI for the project's forge (section 8).
7. Verify: `dune build` and `dune fmt` run clean.

Adjacent scopes: build logic beyond basic stanzas (rules, aliases,
promotion) is `ocaml-dune`; cutting and publishing a release is
`ocaml-release`.

## 2. Required files

| File | Purpose |
|------|---------|
| `dune-project` | Build configuration, opam generation |
| `.ocamlformat` | Code formatting (required) |
| `.gitignore` | VCS ignores (`_build/`, `*.install`, editor files) |
| `LICENSE.md` | License file |
| `README.md` | Project documentation |
| CI config | Tangled / GitHub Actions / GitLab CI |

## 3. Version pins come from installed tools

Exact versions in scaffolding rot. Read them from the environment at setup
time instead of hardcoding remembered numbers:

- `.ocamlformat`'s `version =` line: use the output of
  `dune exec -- ocamlformat --version`.
- `(lang dune X.Y)`: use the major.minor of `dune --version`.
- `(ocaml (>= X.Y))` constraint: use the version the project actually
  develops against (`ocaml -version` via `dune exec -- ocaml -version`).

## 4. Interface files (.mli)

Every library module gets an `.mli`: it is the API boundary, the
encapsulation mechanism, and the documentation surface. Expose only what real
callers need — no reflexive `pp`/`equal`/`compare`/codec menu; each function
must serve an actual workflow (load `ocaml-module-design` when shaping a
signature, `ocaml-doc` for doc-comment style).

```ocaml
(* lib/user.mli *)

(** User management. *)

type t
(** The type for users. *)

val create : name:string -> email:string -> t
(** [create ~name ~email] is a new user. *)

val name : t -> string
(** [name u] is [u]'s name. *)

val pp : Format.formatter -> t -> unit
(** [pp ppf u] formats [u] for users. *)
```

## 5. OCamlFormat configuration

`.ocamlformat` in the project root is required — formatting must be
mechanical so diffs carry only meaning:

```ini
version = <installed ocamlformat version>
```

Run `dune fmt` before every commit.

## 6. Logging setup

When the project uses `logs`, each module declares its own source so
verbosity is controllable per subsystem:

```ocaml
let log_src = Logs.Src.create "project.module"
module Log = (val Logs.src_log log_src : Logs.LOG)
```

Levels: `app` (always shown), `err`, `warn`, `info`, `debug`.

## 7. Project structure

```text
project/
├── dune-project
├── .ocamlformat
├── .gitignore
├── LICENSE.md
├── README.md
├── lib/
│   ├── dune
│   ├── foo.ml
│   └── foo.mli         # required for every library .ml
├── bin/
│   ├── dune
│   └── main.ml
├── test/
│   └── ...             # see ocaml-testing
├── doc/                # Markdown guides, when warranted (see ocaml-doc)
└── CI config           # .github/workflows/, .tangled/workflows/, or .gitlab-ci.yml
```

## 8. dune-project

```lisp
(lang dune <installed major.minor>)
(name project_name)
(generate_opam_files true)

(license ISC)
(authors "Name <email@example.com>")
(maintainers "Name <email@example.com>")
(source (tangled user.domain/project_name))

(package
 (name project_name)
 (synopsis "Short description")
 (description "Longer description")
 (depends
  (ocaml (>= <dev version>))
  (windtrap :with-test)))
```

Do not add `(version ...)` — it is added at release time by the release
tool. Depend on the test framework the project actually uses (`ocaml-testing`
covers the choice).

For GitHub: `(source (github org/repo))`.

## 9. CI

**GitHub Actions**: use the standard `ocaml/setup-ocaml` action with a
matrix over the supported OCaml versions, then `opam install . --deps-only
--with-test`, `dune build`, `dune runtest`.

## Checklist

- [ ] `dune-project`, `.ocamlformat`, `.gitignore`, `LICENSE.md`,
      `README.md`, and CI config exist
- [ ] Versions (`ocamlformat`, `lang dune`, OCaml constraint) read from the
      installed tools, not hardcoded from memory
- [ ] Every library module has an `.mli` exposing only what callers need
- [ ] No `(version ...)` in `dune-project`; opam metadata generated
- [ ] Test suite set up per `ocaml-testing`; CI runs build and tests
- [ ] `dune build` and `dune fmt` run clean
