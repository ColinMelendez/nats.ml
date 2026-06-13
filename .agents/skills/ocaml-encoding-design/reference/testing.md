# Testing an encoding library

Four layers, each catching a different failure mode.

## A. Spec conformance vectors

Vendor them in `test/vectors/` with a `README.md` citing URL + commit SHA.

| Format | Vectors |
|--------|---------|
| JSON (RFC 8259) | [nst/JSONTestSuite](https://github.com/nst/JSONTestSuite) |
| JSON Pointer (6901) / Patch (6902) | examples in the RFCs |
| CBOR (RFC 8949) | Appendix A |
| TOML | [toml-lang/toml-test](https://github.com/toml-lang/toml-test) |
| YAML | [yaml-test-suite](https://github.com/yaml/yaml-test-suite) |
| Protobuf | google/protobuf canonical messages |

Filename prefixes (`y_*` parse, `n_*` reject, `i_*` implementation-defined)
drive the assertion automatically. Fail hard on regression.

## B. Fuzz (alcobar)

```ocaml
(* fuzz/fuzz_foo.ml *)
open Alcobar

let test_crash buf =
  match Foo.of_string codec buf with
  | Ok _ | Error _ -> ()
  | exception Loc.Error.Error _ -> ()
  | exception e -> fail (Fmt.str "unexpected: %a" Fmt.exn e)

let test_roundtrip v =
  let s = Foo.to_string codec v in
  match Foo.of_string codec s with
  | Error e -> fail (Fmt.str "re-decode failed: %a" Loc.Error.pp e)
  | Ok v' -> check_eq ~pp:Foo.Value.pp ~eq:Foo.Value.equal v v'

let suite = "foo", [
  test_case "decode crash safety" [ bytes ] test_crash;
  test_case "roundtrip" [ gen_value ] test_roundtrip;
]

(* fuzz/fuzz.ml *)
let () = Alcobar.run "foo" [ Fuzz_foo.suite ]
```

Invariants: roundtrip; no-crash (arbitrary bytes raise only `Loc.Error.Error`);
reject-stable (same input, same verdict); format-specific ones (CBOR canonical
form, JSON layout preservation). See the `fuzz` skill for the directory
conventions merlint enforces.

## C. Interop against a reference tool

Name the directory after the oracle, not the language
(`test/interop/simdjson/`, not `test/interop/cpp/`). See the `interop-testing`
skill.

| Format | Oracle |
|--------|--------|
| JSON | simdjson (C++), serde_json (Rust) |
| CBOR | python-cbor2, libcbor |
| YAML | libyaml |
| TOML | toml-rs |

## D. Checked documentation examples

Every first-party `README.md` and every public `.mli` code block is a test. Wire
`mdx` into `@runtest` for both (see the `mdx` skill for the per-dune-file rule):

```lisp
(using mdx 0.4)

(mdx (files README.md) (libraries foo))

(mdx (files foo.mli codec.mli value.mli error.mli sort.mli) (libraries foo))
```

When first adding mdx, prove the examples are compiled rather than merely
present: change `Foo.Codec.string` to `Foo.Codec.strnig` in one example, confirm
`dune build @runtest` fails with a name error, then revert.

Examples must use the real public API shape:

```ocaml
module C = Foo.Codec

let codec =
  C.Object.map make
  |> C.Object.member "name" C.string ~enc:(fun x -> x.name)
  |> C.Object.seal
```

Never show stale shortcut APIs (`Foo.Object`, `Foo.string`) unless they exist
and are the intended surface. Examples are stronger than prose: if they imply a
different API, users believe the example.

Do not add native mdx checks for browser-only `.mli` files or README examples
needing JavaScript primitives (`brr`, `Jv`) unless the runner is a JS runtime.
Keep them absent, mark them skipped with a reason, or test them through a
js_of_ocaml target.

## E. Benchmarking

Plain executables, hand-rolled timing, `memtrace` for allocations. No
`bechamel` / `core_bench` dep. See `ocaml-crc/bench/`, `ocaml-git/bench/`.

```ocaml
let time f =
  Gc.compact ();
  let t0 = Unix.gettimeofday () in
  let r = f () in r, Unix.gettimeofday () -. t0

let run name size f =
  ignore (f ());                                  (* warm up *)
  let runs = Array.init 20 (fun _ -> snd (time f)) in
  Array.sort compare runs;
  let median = runs.(10) in
  let mbs = float_of_int size /. median /. 1e6 in
  Fmt.pr "%-30s  %6.1f MB/s  (%6.2f ms)@." name mbs (median *. 1e3)
```

Targets: simdjson (JSON), libyaml (YAML), libcbor (CBOR). When the
implementation has a backend choice, compare the hand-written scanner with
Angstrom's buffered driver (and the unbuffered driver only when it is a
supported production path), and compare direct writer output with Faraday
where applicable. For binary formats, compare the relevant `Bytes` path with a
`Bigstringaf.t`/bigstring path, and add Cstruct views only when they are part of
the proposed implementation. Vary reader slice sizes and measure throughput,
allocations, peak memory, copy counts, skip/filter behavior, and output-buffer
lifetimes. Benchmarks catch regressions; they are not shootouts.

## Browser fast-path (JSON only)

Only JSON has browser-native parsing; other formats get no `.brr` sub-library.

The shape mirrors the core six verbs with the native types swapped in: `Jstr.t`
parallels `string`, `Jv.t` parallels `Bytesrw.Bytes.Reader.t` (the zero-copy path). Put
it in a separate public library and module (`json.brr` exposing `Json_brr`), not
a nested `Json.Brr`, so the core stays dependency-free and opam deps stay
honest.

```ocaml
module Json_brr : sig
  val of_jstr     : 'a Json.codec -> Jstr.t -> ('a, Loc.Error.t) result
  val of_jstr_exn : 'a Json.codec -> Jstr.t -> 'a
  val to_jstr     : ?indent:int -> ?preserve:bool -> 'a Json.codec -> 'a -> Jstr.t

  val of_jv     : 'a Json.codec -> Jv.t -> ('a, Loc.Error.t) result
  val of_jv_exn : 'a Json.codec -> Jv.t -> 'a
  val to_jv     : 'a Json.codec -> 'a -> Jv.t
end
```

No `decode_*` / `encode_*` naming (reserved for the pure `Value.t` layer), no
`'` variants, no `recode_*`. Always return `Loc.Error.t`, never `Jv.Error.t`.
`Json.of_string` still works in the browser, just slower.

## Prior art

| Library | Value | Codec | Cursor |
|---------|-------|-------|--------|
| Scala `circe` | `Json` | `Encoder[A]` / `Decoder[A]` | `Cursor` / `HCursor` |
| Haskell `aeson` | `Value` | `ToJSON` / `FromJSON` | `lens-aeson` |
| Rust `serde_json` | `Value` | `Serialize` / `Deserialize` | `pointer` (RFC 6901) |
| Rust `simdjson` | tape | - | On-Demand |
| OCaml `jsont` (dbuenzli) | `Value.t` | `'a Codec.t` | - |
| OCaml `Yojson` | polymorphic variant | - | - |
| OCaml `data-encoding` (Tezos) | - | `'a t` (combinators) | - |
| Go `encoding/json` | native | reflection | `RawMessage` |

- **circe**: cleanest design for ML-shaped languages; this skill's four-layer
  model is circe's with a top-level IO layer added.
- **jsont**: the direct OCaml reference: GADT-backed codecs, the `Value.t` /
  `Codec.t` split, per-node `Meta.t`.
- **serde** (Tolnay): codec-first philosophy, `Value` as escape hatch.
- **aeson**: AST-first tradeoff; later added a streaming `Encoding` layer,
  which we skip because direct codec-to-bytes already does the job.
- **simdjson** (Lemire): tape representation and On-Demand API; the performance
  reference.
- **ocaml-loc** (monorepo): the extensible `Error.kind` used throughout.
