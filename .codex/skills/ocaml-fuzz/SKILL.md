---
name: fuzz
description: "Property and fuzz tests with Alcobar (Crowbar fork) for OCaml packages. Use when adding a fuzz/ directory, writing crash-safety or roundtrip tests for a parser/codec/state machine, fixing a merlint E7xx fuzz-layout finding, or running an AFL campaign."
license: ISC
metadata:
  copyright: "Copyright (c) 2026 Thomas Gazagnaire <thomas@gazagnaire.org>"
---

# Fuzz Testing with Alcobar

`alcobar` is a Crowbar fork whose runner mirrors `Alcotest.run`. One binary serves three modes, selected automatically from
`Sys.argv`: an Alcotest-style property run under `dune test`, corpus generation
under `--gen-corpus DIR`, and an AFL child when the last argument is a file.

There is **no `gen_corpus.ml`** and no per-module `run ()`; both are stale
patterns. Corpus seeds come from `fuzz.exe --gen-corpus`, which writes the exact
bytes the generators consumed during a passing run.

## Layout

`fuzz/` is a **sibling of `test/`**, never nested inside it.

```txt
ocaml-foo/
  lib/
  test/
  fuzz/
    dune
    fuzz.ml           # runner: collects Fuzz_*.suite
    fuzz_foo.ml       # tests lib/foo.ml
    fuzz_foo.mli      # exports only [suite]
    fuzz_bar.ml(i)    # tests lib/bar.ml
```

One `fuzz_<module>.ml` per library module, named after the module it tests.

## The three files

**`fuzz/dune`**, one `(executable ...)`, plus a `runtest` rule for the property
run and a `fuzz` rule for the AFL campaign, gated on the `afl` profile so they
never both fire:

```lisp
(executable
 (name fuzz)
 (libraries foo alcobar fmt))

(rule
 (alias runtest)
 (enabled_if
  (<> %{profile} afl))
 (deps fuzz.exe)
 (action
  (run %{exe:fuzz.exe})))

(rule
 (alias fuzz)
 (enabled_if
  (= %{profile} afl))
 (deps fuzz.exe)
 (action
  (progn
   (run %{exe:fuzz.exe} --gen-corpus corpus)
   (run afl-fuzz -V 60 -i corpus -o _fuzz -- %{exe:fuzz.exe} @@))))
```

In a multi-package dune project both rules need `(package foo)` so dune knows
which package owns them; the executable itself stays private (E727).

**`fuzz/fuzz.ml`**; the runner does nothing but collect suites:

```ocaml
let () = Alcobar.run "foo" [ Fuzz_foo.suite; Fuzz_bar.suite ]
```

**`fuzz/fuzz_foo.mli`**: exactly one export:

```ocaml
(** Fuzz tests for {!Foo}. *)

val suite : string * Alcobar.test_case list
(** Test suite. *)
```

**`fuzz/fuzz_foo.ml`**: test functions at the top, one `suite` at the bottom.
The suite's name must match the filename: `fuzz_foo.ml` declares
`("foo", [...])` (E725).

```ocaml
open Alcobar

(* Classification and conversion agree on every byte. *)
let test_classify_value c =
  let classified = Ascii.is_hex_digit c in
  let valued = Ascii.hex_value c <> None in
  if classified <> valued then
    failf "is_hex_digit %C = %b but hex_value = %b" c classified valued

(* hex_char then hex_value is the identity on the nibble range; out of range
   always raises. *)
let test_roundtrip v =
  if v >= 0 && v <= 15 then
    match Ascii.hex_value (Ascii.hex_char v) with
    | Some v' when v' = v -> ()
    | Some v' -> failf "roundtrip %d = %d" v v'
    | None -> failf "hex_char %d not a hex digit" v
  else
    match Ascii.hex_char v with
    | exception Invalid_argument _ -> ()
    | c -> failf "hex_char %d = %C instead of raising" v c

let suite =
  ( "ascii",
    [
      test_case "classify agrees with value" [ char ] test_classify_value;
      test_case "hex_char roundtrip" [ int8 ] test_roundtrip;
    ] )
```

A passing test returns `()`; there is no need for a trailing `check true`.

## The four properties worth testing

Everything below is a variation on these. A suite with none of them provides no
coverage (E726).

**Crash safety**: a parser must not raise anything unexpected on arbitrary
input. Invalid input returning `Error` is a pass, not a failure.

```ocaml
let test_decode buf =
  match Foo.decode buf with
  | Ok _ | Error _ -> ()
  | exception Loc.Error.Error _ -> ()          (* the declared exception *)
  | exception e -> failf "unexpected: %a" Fmt.exn e
```

**Roundtrip**: anything that decodes must re-encode to something that decodes
back to the same value. A failed *initial* decode is fine; a failed *re-decode*
is a bug.

```ocaml
let test_roundtrip buf =
  match Foo.decode buf with
  | Error _ -> ()                              (* input was invalid, fine *)
  | Ok v ->
      match Foo.decode (Foo.encode v) with
      | Error _ -> fail "re-decode failed"
      | Ok v' -> check_eq ~pp:Foo.pp ~eq:Foo.equal v v'
```

**Boundaries and rejection**: smart constructors accept exactly their valid
range and reject everything else. Drive both directions from one generator
rather than hand-writing min/max cases.

```ocaml
let test_apid n =
  match Apid.of_int n with
  | Some a -> if Apid.to_int a <> n then failf "roundtrip %d" n
  | None -> if n >= 0 && n <= 2047 then failf "rejected valid %d" n

let suite = ("apid", [ test_case "of_int" [ range ~min:(-1000) 4000 ] test_apid ])
```

**State transitions**: valid transitions land in the expected state, invalid
ones return the expected error constructor (not just any error).

```ocaml
let test_activate_empty_fails kid algo =
  let key = Key.empty ~kid ~algorithm:algo in
  match Key.activate key with
  | Ok _ -> fail "should fail on an Empty key"
  | Error (Key.Invalid_state_transition _) -> ()
  | Error _ -> fail "wrong error constructor"
```

Pretty-printers get the same crash-safety treatment: decode, then
`Fmt.str "%a" Foo.pp v` and discard.

## Generators

| Generator | Type | Notes |
|-----------|------|-------|
| `bytes` | `string` | arbitrary binary input; the workhorse |
| `bytes_fixed n` | `string` | exactly `n` bytes |
| `int` `int8` `uint8` `int16` `uint16` `int32` `int64` | ints | full range for the width |
| `float` `bool` `char` `uchar` | scalars | |
| `range ?min n` | `int` | `min` inclusive to `min + n` exclusive; raises for `n <= 0` |
| `const v` | `'a` | fixed value, for tests with no random input |
| `list` / `list1`, `array` / `array1` | containers | `1` suffix means non-empty |
| `option` `pair` `result` | wrappers | |
| `choose gens` | `'a` | pick a generator arbitrarily |
| `shuffle l` | `'a list` | permutations of a fixed list |
| `map gens f` | `'b` | derive from other generators |
| `fix` / `unlazy` | `'a` | recursive generators |
| `with_printer pp gen` | `'a` | better failure output without a `~pp` at each `check_eq` |
| `dynamic_bind` | `'a` | generator depending on a generated value; avoid unless required (it is opaque to the library's analysis) |

Assertions: `check : bool -> unit`, `check_eq ?pp ?cmp ?eq`, `fail : string ->
'a`, `failf` (with `%a` support). To abandon a sample without recording a
failure: `guard : bool -> unit`, `bad_test : unit -> 'a`, `nonetheless : 'a
option -> 'a`.

`Alcobar.Syntax` gives `let+` / `and+` / `let*` over generators when a
hand-written `map [ ... ]` gets unwieldy.

## Merlint rules

| Rule | What it checks |
|------|----------------|
| E700 | `fuzz.ml` collects `Fuzz_*.suite`; it never defines `test_case` inline |
| E705 | Every `fuzz_*.ml` has an `.mli` exporting only `suite` |
| E710 | Every `fuzz_<module>.ml` has a matching library module `<module>.ml` |
| E715 | Every fuzz module is referenced from `fuzz.ml` |
| E718 | Only `fuzz.ml` / `fuzz_*.ml` in `fuzz/`; the dune rule uses `fuzz.exe --gen-corpus` |
| E720 | Exactly one executable stanza per fuzz directory |
| E721 | `fuzz/` is a sibling of `test/`, not nested inside it |
| E722 | `(executable ...)` with explicit rules, never a `(test ...)` stanza |
| E724 | Both `(rule (alias runtest) ...)` and `(rule (alias fuzz) ...)` present |
| E725 | `suite` name matches the filename (`fuzz_foo.ml` -> `("foo", ...)`) |
| E726 | The suite is non-empty |
| E727 | In a multi-package project, both rules carry `(package PKG)` |

## Running

```sh
dune test                                    # property run, part of @runtest
dune build @fuzz --profile=afl               # generate corpus + run AFL for 60s
```

For long campaigns across many targets, `crow` orchestrates AFL:

```sh
crow init                       # writes dune-workspace with the afl profile
dune build --profile=afl @fuzz
crow list
crow start --cpus=8 --duration=24h
crow status
crow stop
```

## Two mistakes that cost a build

**An empty generator list with a function.** `test_case "t" [] (fun () -> ...)`
does not typecheck ("This expression should not be a function"). Use
`[ const () ]` for a test with no random input.

**Failing on invalid input.** `| Error _ -> fail "decode failed"` in a roundtrip
test is wrong: the fuzzer's whole job is to feed input that does not decode.
Only a failure *after* a successful decode is a bug.

## Coverage

For each module with a public `.mli`, work down this list and stop when the
remaining items don't apply:

- Crash safety on every `decode_*` / `parse_*` / `read_*` / `of_*`.
- Roundtrip on every `encode`/`decode` and `to_*`/`of_*` pair.
- Boundaries and rejection on every smart constructor and constrained type.
- All state-machine transitions, valid and invalid.
- `pp_*` crash safety; `equal` and `compare` consistency.

Add fuzz coverage in this order when retrofitting a codebase: security-critical
code (crypto, authentication, key management) first, then protocol parsers,
state machines, constrained types, and utility conversions last.
