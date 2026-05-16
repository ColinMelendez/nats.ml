---
name: ocaml-encoding-design
description: "Module shape for OCaml codec libraries (JSON, YAML, TOML, CBOR, MsgPack, Bencode, Sexp, XML, protobuf, BSON). Use when creating or restructuring an encoding library, deciding where of_string/decode belong, naming _exn vs prime variants, splitting sub-libraries, or reviewing a codec's public API for consistency. Sister skill to ocaml-protocols (stateful wire protocols)."
license: ISC
metadata:
  copyright: "Copyright (c) 2026 Thomas Gazagnaire <thomas@gazagnaire.org>"
---

# OCaml Encoding Library Design

Applies to every library that (de)serializes a structured format.

## The four layers

```txt
Foo.Value    AST. Foo.t = Foo.Value.t. Constructors, pp, equal, IO shortcuts.
Foo.Codec    Bidirectional combinators. 'a Foo.codec = 'a Codec.t. Pure.
Foo.Cursor   Zipper over Value. Every AST gets one.
Foo          of_string / to_string / of_reader / to_writer / decode / encode.
```

Each layer is usable alone; each higher one is a thin wrapper over the one below.

Deep material lives beside this file, read it when you reach that layer:

- [`reference/errors.md`](reference/errors.md): the `Foo.Error` facade over `Loc.Error`.
- [`reference/streaming.md`](reference/streaming.md): `Codec.skip`, `Cursor.of_reader`, `Foo.Stream`, parser/serializer backends, layout-preserving transform.
- [`reference/testing.md`](reference/testing.md): spec vectors, fuzz, interop, mdx, benchmarks, browser fast-path, prior art.

## Public surface

Write `foo.mli` as the API you want odoc readers to see. Never
`include module type of Codec`; it imports every helper submodule into the
manual and makes construction machinery look endorsed.

```ocaml
(* foo.ml re-exports the implementation module. *)
module Codec = Codec
```

```ocaml
(* foo.mli restates the public subset explicitly, in reading order.
   codec.mli may still expose the fuller vocabulary siblings need. *)
module Codec : sig
  type 'a t = 'a Codec.t
  val bool : bool t
  val string : string t
  module Object : sig ... end
  module Value : sig val t : value t end
  val decode : 'a t -> value -> ('a, Error.t) result
  val encode : 'a t -> 'a -> value
end
```

Odoc is the API review surface. After changing a signature, build `@doc` and
read the module tree: helper subpages like `Codec.Base`, `Object.Member`, or
`Internal` on the first page mean the public API is leaking implementation.

## Layer 1: `Foo.Value`

Concrete ADT matching the format's data model. Every constructor carries
`Loc.Meta.t` through the `'a node = 'a * Meta.t` wrapper.

```ocaml
module Value : sig
  module Meta = Loc.Meta
  type 'a node = 'a * Meta.t

  type t =
    | Null   of unit node
    | Bool   of bool node
    | Number of float node
    | String of string node
    | Array  of t list node
    | Object of (name * t) list node

  val pp    : Format.formatter -> t -> unit
  val equal : t -> t -> bool

  val of_string     : string -> (t, Loc.Error.t) result
  val of_string_exn : string -> t
  val to_string     : ?indent:int -> ?preserve:bool -> t -> string
  val of_reader     : Bytesrw.Bytes.Reader.t -> (t, Loc.Error.t) result
  val of_reader_exn : Bytesrw.Bytes.Reader.t -> t
  val to_writer     :
    ?indent:int -> ?preserve:bool -> eod:bool -> t ->
    Bytesrw.Bytes.Writer.t -> unit
end
```

Per-node (not per-root) `Meta.t` buys three things: byte-precise decoder errors
(`users[3].email: expected string, found number (line 47, col 18)`), layout
preservation under `~preserve:true`, and a location for every node that linters
and formatters walk. The cost is one word per node. Parsers populate it from the
UTF-8 decoder's offset tracking; `Loc.Meta.none` is the sentinel for values
built programmatically, and encoders accept it silently.

### `?indent` and `?preserve` are the only format encoding knobs

Every `to_string` / `to_writer` in every format takes both, so tooling written
against one codec works against all of them:

- `?indent:int`, absent means compact (no whitespace); `~indent:2` indents
  nested structures by that many spaces per level. Governs nodes *without*
  source Meta.
- `?preserve:bool`, default `false`. When `true`, nodes carrying a non-`none`
  `Meta.t` reproduce their source whitespace byte-for-byte; nodes with
  `Meta.none` fall back to `?indent`. So `~preserve:true ~indent:2` means
  "preserve unchanged subtrees, pretty-print new ones."

```ocaml
(* BAD: closed enum can't express indent width or preserve+indent *)
val to_string : ?format:[`Minify|`Indent|`Layout] -> ...
(* BAD: two verbs proliferate; users grep the wrong one *)
val to_string_pretty : ...
(* BAD: inverted default; users opt out of pretty *)
val to_string : ?compact:bool -> ...
```

Stream-boundary controls are separate from format encoding. In particular,
`to_writer` takes an explicit `~eod:bool`: a writer may be reused for another
document when `false`, or receives the byte-stream end marker when `true`.
Never hide that choice in a writer helper.

## Layer 2: `Foo.Codec`

GADT-backed. `'a Codec.t` is a bidirectional map between format values and
OCaml type `'a`. Pure, no IO.

### The GADT describes the FORMAT, not the OCaml value

The most common mistake in codec libraries: naming constructors after OCaml
features (`Record`, `Variant`, `Tuple`) instead of the format's data model.

```ocaml
(* GOOD: mirrors the JSON data model *)
type _ t =
  | Null    : unit t
  | Bool    : bool t
  | Number  : float t
  | String  : string t
  | Array   : 'a t -> 'a list t
  | Object  : ('o, 'o) obj -> 'o t
  | Map     : ('a -> 'b) * ('b -> 'a) * 'a t -> 'b t

(* BAD: OCaml-centric; can't represent a JSON value that isn't an OCaml record *)
type _ t =
  | Record : 'a record_descr -> 'a t
  | Variant : 'a variant_descr -> 'a t
```

The format side is fixed by the spec (RFC 8259, XML 1.0); the OCaml side is
arbitrary and user-provided. The GADT's alphabet spells out the format;
`map` / `case` / `obj` bridge to the user's shape. Daniel Bünzli's
[*An alphabet for your data soups*](https://erratique.ch/software/jsont) calls
the primitives "an alphabet" for exactly this reason.

Some libraries invert this: Tezos `data-encoding`, Irmin `repr`, Rust `serde`
mirror language constructs (`tup4`, `obj6`, `union`) to project arbitrary types
through many backends. **Do not follow that here.** Universal combinators
cannot capture format quirks (JSON `null` vs missing member, XML mixed content,
TOML typed dates, attribute ordering, comments), and schema generation, diffing
and format-native tooling all work on the format's alphabet.

### Never materialize `Value.t` just to query it

```ocaml
(* BAD: allocates the full AST to read one field *)
let extract_email s =
  match Xml.Value.of_string s with
  | Error _ -> None
  | Ok el ->
      Xml.Value.find "user" el
      |> Option.bind (fun u -> Xml.Value.find "email" u)
      |> Option.map Xml.Value.text

(* GOOD: codec reads only what's needed, bytes straight to 'a *)
let email_codec =
  Xml.element "root" (Xml.child "user" (Xml.child "email" Xml.string))
```

`Value.t` is the escape hatch for genuinely dynamic use (unknown schemas,
generic transforms, layout-preserving edits). It is not a query layer.

### `codec.ml` depends on `value.ml`, never the reverse

The rule above is about *typed* codecs, not a claim that codecs are ignorant of
the AST: the identity codec `Foo.Codec.Value.t` does build a `Value.t`. The
one-way edge is:

```txt
core.ml    : private helpers (resizable buffers, Fmt, parser backend, byte scanning). No Value/Codec deps.
sort.ml    : closed enum of format node categories. Just Fmt.
value.ml   : AST type + Meta + pp/equal/queries. Does NOT reference Codec.
codec.ml   : codec GADT + typed parse/encode interpreters. `type value = Value.t`.
cursor.ml  : zipper over value.ml.
foo.ml     : the six verbs; re-exports the modules above.
```

This is the [jsont](https://erratique.ch/software/jsont) layout. Merlint
**E945** enforces it for data codecs and for
[`ocaml-protocols`](../ocaml-protocols/SKILL.md), which share the layering
(`value.ml` for data, `message.ml` for a protocol). It reads the typedtree, so
an AST using ocaml-wire's external `Wire.Codec` is not flagged; only a genuine
sibling-`Codec` reference is.

A `value.ml` that starts importing `Codec` usually means a parse entry point
belonging at the top level (`Foo.of_string`) was put on `Value` directly.

### One parse kernel, two readers

The private parse kernel carries no AST type and owns the format's structural
primitives: lookahead, whitespace, atoms, quoted strings, UTF-8 offsets, and
byte-level `skip_value`. It may be a hand-written scanner or an incremental
parser backend such as
[Angstrom](https://ocaml.org/p/angstrom/0.16.1/doc/angstrom/Angstrom/index.html).
Keep it shared by:

- `value.ml`'s tree builder `parse : parser -> Value.t`;
- `codec.ml`'s streaming interpreter `of_stream : 'a codec -> parser -> 'a`.

`of_string` / `of_reader` run `of_stream` over the parse kernel in one pass. For
an object with `skip_unknown`, `of_stream` byte-skips discarded members instead
of building them; every other shape delegates to the tree walker on a
materialised sub-value, so results and errors are identical and only kept
members are built. An Angstrom parser that materialises a complete `Value.t`
is not a substitute for the typed interpreter: using it for `Codec.skip` would
reintroduce the allocation the streaming path is meant to avoid.

```ocaml
(* If you write this inside of_string, stop -- it builds the whole tree
   and defeats the skip. *)
let v = parse_value s in interpret_codec codec v
```

Two readers over one parse kernel is not two parsers, and a private scanner or
Angstrom adapter is not a banned abstraction. What is banned is a *public*
lexeme/token-stream API
(jsonm-style events as a third surface beside `Value` and `Codec`). Both
runtimes are correct and expected: `decode : 'a codec -> Value.t -> 'a` walks an
AST you already hold; `of_string` streams bytes in one pass and is never
implemented as `decode codec (Value.of_string s)`.

For high-throughput binary or protocol formats, prefer `Bigstringaf.t`
(`bigstring`) at a private parser/serializer boundary when the surrounding IO
already uses it. A bigstring is a byte `Bigarray.Array1.t` with C layout;
`Cstruct.t` is an optional offset/length view with binary-field accessors over
that kind of storage. This is a backend recommendation, not a universal public
representation rule: keep `string`/`Bytes` convenience APIs where they fit,
and benchmark the complete pipeline, including conversions and buffer
lifetimes.

### Finally tagged

The combinator style is Bünzli's **finally tagged**, not Kiselyov's *tagless
final* (the opposite: tagless final evaluates straight into a semantic domain
with no intermediate representation). Combinators construct a GADT; interpreters
walk it to encode, decode, pretty-print, generate schemas, diff. Adding an
interpreter adds a walker; adding a combinator extends the GADT and the compiler
names every interpreter that must handle it.

Name it in the module doc:

```ocaml
(** XML codecs -- {e finally tagged} combinators. [Codec.t] is a GADT;
    interpreters walk it to encode, decode, and introspect. *)
```

Alias the codec type at the top of `foo.mli`. The `'a` keeps it visually
distinct from `Foo.t`; never expose a bare unparameterized `Foo.codec`:

```ocaml
type 'a codec = 'a Codec.t
```

### Descent combinators stamp `Loc.Path` steps

Every codec is a navigation rule: "at this position, apply this sub-codec". Each
format names its descent combinators after its own `Sort`, but they compile to
the same two path steps.

| Format | Name-addressed              | Index-addressed     | Path step             |
|--------|-----------------------------|---------------------|-----------------------|
| JSON   | `Codec.mem "key" inner`     | `Codec.nth n inner` | `Mem "key"` / `Nth n` |
| XML    | `Codec.element "tag" inner` | `Codec.nth n inner` | `Mem "tag"` / `Nth n` |
| TOML   | `Codec.table "name" inner`  | `Codec.nth n inner` | `Mem "name"` / `Nth n`|
| CBOR   | `Codec.key "k" inner`       | `Codec.nth n inner` | `Mem "k"` / `Nth n`   |
| Sexp   | `Codec.labeled "n" inner`   | `Codec.nth n inner` | `Mem "n"` / `Nth n`   |

That makes three views of one navigation agree:

```txt
Codec combinator                Cursor move                   Path step
Codec.mem "email" inner         Cursor.down_field "email"     Path.Mem "email"
Codec.nth 3 inner               Cursor.down_index 3           Path.Nth 3
```

A decode failure inside `Codec.mem "email"` carries the same `Path.Mem "email"`
frame that `Cursor.to_context` produces, so cross-format tooling walking error
paths sees a uniform `Loc.Path.t`.

### The identity codec lives at `Foo.Codec.Value.t`

```ocaml
val Foo.Codec.Value.t : Foo.t Foo.codec
```

Not `Foo.Value.codec`: users build codecs in `Foo.Codec`, so the generic AST
codec belongs beside the scalar and object codecs. `Foo.Value.of_string` stays
as a convenience wrapper over `Foo.of_string Foo.Codec.Value.t`.

## Layer 3: `Foo.Cursor`

Every AST gets a cursor: a zipper over `Value.t` with focus and context. Query
paths, patch operations and layout-preserving edits all compose through it.
**Never skip it**, even for formats nobody queries interactively (protobuf,
BSON) it powers patches, structured error paths, and tooling, for the cost of
one small module.

```ocaml
module Cursor : sig
  type t
  val root       : Value.t -> t
  val focus      : t -> Value.t
  val down_field : string -> t -> t option
  val down_index : int -> t -> t option
  val up         : t -> t option
  val set        : Value.t -> t -> t
  val modify     : (Value.t -> Value.t) -> t -> t
  val top        : t -> Value.t            (* reconstruct root *)
  val path       : t -> Loc.Path.t

  val of_pointer : string -> t -> t option (* format-specific path syntax *)
  val to_pointer : t -> string
end
```

`Loc.Context.t` (the trail attached to every error) and `Cursor.frame list` are
the same structure, the context adding kind-of-parent labels (`"<root>"`,
`"list of <n>"`). Expose the crossover so a failed decode jumps straight to the
offending node:

```ocaml
val Cursor.to_context : t -> Loc.Context.t
val Cursor.of_context : Loc.Context.t -> Value.t -> t option
(* Error.context e |> Cursor.of_context |> Option.map Cursor.focus *)
```

### Sort vs kind

Keep the two separate in vocabulary and API.

**Sort** is the closed enumeration of node categories the format has: a fixed
algebraic type answering "which grammar non-terminal is this".

| Format | Sort.t |
|--------|--------|
| JSON | `Null \| Bool \| Number \| String \| Array \| Object` |
| XML  | `Element \| Attribute \| Text \| Document` |
| TOML | `String \| Integer \| Float \| Bool \| Datetime \| Array \| Table` |

**Kind** is a human-readable label for one instance: a `string` built from a
sort plus an identifier (`"<users>"`, `"list of <n>"`, `"member email"`),
answering "which specific node". It is what ends up in `Loc.Context.push_*`.

```ocaml
module Sort : sig
  type t = ...
  val to_string : t -> string
  val kinded : kind:string -> t -> string    (* combine into a kind-label *)
end

let element_kind_node tag = (Sort.kinded ~kind:(Fmt.str "<%s>" tag) Sort.Element, Meta.none)
```

Keep `Sort` and the kind-node helpers together in the error/context module so
enum -> label -> error context lives in one place.

### Thread `?kind` through primitive combinators

Every combinator taking user-supplied encode/decode functions accepts
`?(kind = "") ?(doc = "")`. The kind is stored in the codec and surfaces in the
default error when the other direction is missing:

```ocaml
let map ?(kind = "") ?(doc = "") ?dec ?enc ?(enc_meta = enc_meta_none) () =
  let dec = match dec with
    | Some dec -> dec
    | None ->
        let kind = Sort.kinded' ~kind base_map_sort in
        fun meta _v -> Error.no_decoder meta ~kind
  in
  ...
```

`Sort.kinded' ~kind base_map_sort` yields `"<user-kind> <base-sort>"` when the
user supplied a kind, else just `"<base-sort>"`, so users read
`No decoder for user_id map` rather than `decode not supported`. The same
applies to `map`, `any`, `option`, `of_of_string`, object builders.

### Match the format's path spec

If the format has a canonical path language, implement it exactly: same syntax,
same semantics, same edge cases.

| Format | Spec |
|--------|------|
| JSON | RFC 6901 (Pointer), RFC 9535 (JSONPath), RFC 6902 (Patch) |
| XML | XPath 1.0 / 3.1 (W3C) |
| TOML | dotted keys per TOML 1.1 + `[N]` index (library convention) |
| YAML | yq-convention paths (no RFC) |
| Protobuf | `google.protobuf.FieldMask` dotted-field syntax |
| BSON | MongoDB dot-notation field paths |
| CBOR | CDDL path syntax where applicable |

When two specs cover one format (JSON has 6901 *and* 9535), implement both as
separate `of_pointer` / `of_path` entry points: a pointer navigates one node, a
path can match many, so don't collapse them. Document which RFC you target;
conformance vectors verify the match.

Where no standard exists (S-expressions, Bencode, MsgPack), lift RFC 6901's
grammar: it is well-specified, handles empty keys and `~` escaping, and readers
already know it.

Streaming (`Codec.skip`, `Cursor.of_reader`, `Foo.Stream`) is in
[`reference/streaming.md`](reference/streaming.md).

## Layer 4: top-level `Foo`

Six verbs: four for IO, two for the pure codec layer. Each `of_` / `decode` has
an `_exn` twin for the allocation-free fast path.

```ocaml
val of_string     : 'a codec -> string -> ('a, Loc.Error.t) result
val of_string_exn : 'a codec -> string -> 'a
val to_string     : ?indent:int -> ?preserve:bool -> 'a codec -> 'a -> string

val of_reader     : 'a codec -> Bytesrw.Bytes.Reader.t -> ('a, Loc.Error.t) result
val of_reader_exn : 'a codec -> Bytesrw.Bytes.Reader.t -> 'a
val to_writer     :
  ?indent:int -> ?preserve:bool -> eod:bool -> 'a codec -> 'a ->
  Bytesrw.Bytes.Writer.t -> unit

val decode     : 'a codec -> Value.t -> ('a, Loc.Error.t) result
val decode_exn : 'a codec -> Value.t -> 'a
val encode     : 'a codec -> 'a -> Value.t
```

`to_` / `encode` rarely fail, so no `_exn` twin. `_exn` raises
`Loc.Error.Error`: shared across every format, never per-library.

Document the input-consumption contract. `of_string` normally requires one
complete document and no trailing bytes; `of_reader` must say whether it
decodes one document and leaves the reader positioned at the next byte, or
requires end-of-data. If both policies are needed, expose that as an explicit
consumption policy rather than silently choosing based on the reader backend.

## Naming

### `_exn` for raising, never `'`

```ocaml
val of_string_exn : ... -> 'a      (* not of_string' *)
```

`'` is acceptable in exactly two cases:

1. **Configuration-variant pair**: a base function with a default plus a primed
   variant taking the config as a *required positional* argument, because `%a`
   cannot consume optional arguments:

   ```ocaml
   val pp  : t Fmt.t
   val pp' : number_format -> t Fmt.t
   ```

2. **Keyword escape for a format-native name**: when the format's own sort or
   token name collides with an OCaml keyword:

   ```ocaml
   val object' : mem list cons    (* JSON "object" sort *)
   ```

Never as a rename (`decode'` for "a different decoder"), never as a
raise-variant, never as a single-use variant without a paired default. And
codec-layer combinators are not format sorts: `Rec` / `Map` / `Any` / `Ignore`
are plumbing, so they take descriptive names (`fix`, `map`) and never a
keyword-escape `'`.

### `of_X` / `to_X` for IO; `decode` / `encode` for codec-over-Value

The six verbs are the complete public vocabulary. No synonyms:

```
parse / from_string   -> of_string        read / input    -> of_reader
print / unparse       -> to_string        write / output  -> to_writer
marshal / unmarshal   -> to_string / of_string
```

Uniform verbs across every library in the monorepo beat matching each upstream's
preference.

### Grow arguments, not verbs

```ocaml
(* GOOD *)
type meta = [ `None | `Locs | `Full ]
val of_string : ?meta:meta -> 'a codec -> string -> ...

(* BAD *)
val of_string_layout : 'a codec -> string -> ...
val of_string : ?layout:bool -> ?locs:bool -> 'a codec -> string -> ...
```

One sum type for coupled parse metadata. Two booleans create four apparent
states and force users to rediscover the invalid combinations.

### `Foo.t = Foo.Value.t`

```ocaml
type t = Value.t          (* not `type 'a t = 'a Codec.t` (jsonm-style) *)
type 'a codec = 'a Codec.t    (* and not an abstract `type t` (serde-style) *)
```

### Pretty-printers: `type x -> pp_x`, `type t -> pp`

One printer per exposed type, named for the type, except the distinguished `t`
which gets a bare `pp`. Banned: `print_x`, `show_x`, `format_x`, and `pp` for
any type other than `t`. Expose only the printers callers actually need.

A codec-driven formatter must be directly usable with `%a`, no dummy `unit ->`
argument:

```ocaml
val pp_value : ?number_format:number_format -> 'a codec -> 'a Fmt.t
(* Fmt.pr "%a@." (Foo.pp_value codec) v *)
```

## Errors

`Foo.Error` is a **facade over `Loc.Error`**, not a competing hierarchy: it
extends `Loc.Error.kind` with typed constructors, registers printers at module
init, re-exports the `Loc.Error` surface, and provides a
`(builder, fail_builder)` pair for every recurring error shape.

Never model errors as a sealed ADT with its own exception; that duplicates
`Loc.Error`, forecloses third-party extension, and invents a parallel `location`
record instead of reusing `Loc.Meta.t` + `Loc.Context.t`.

Full pattern, helper menu, and the banned list: [`reference/errors.md`](reference/errors.md).

## Parser failure and result at the boundary

Building `result` during recursive parsing allocates `Ok _` at every tree level.

```ocaml
let of_string_exn codec s =
  parse_value (Bytesrw.Bytes.Reader.of_string s) |> interpret_codec codec

let of_string codec s =
  match of_string_exn codec s with
  | v -> Ok v
  | exception Loc.Error.Error e -> Error e
```

Both share the hot path. The internal exception is private to the hand-written
parser; only `Loc.Error.Error` surfaces to callers. An incremental backend has a
different control state: Angstrom's `Partial` means "need more input", not a
format error, while `Fail` becomes `Loc.Error` only once the driver has enough
input to decide that parsing cannot continue. Do not force a `Partial` state
through an exception or a user-visible error result.

## UTF-8

Every text format mandates UTF-8. Validate incrementally with `uutf` as bytes
are consumed, never by buffering the whole input to pre-validate it. Normalize
with `uunf` only where the spec requires (JSON object-key comparison, RFC 8259
§8.3; XML `xml:id`), over-normalizing changes user data.

Codepoint rejection is format-specific: maintain a predicate and raise at the
exact offset (XML forbids most C0 controls; TOML rejects raw control chars in
strings). On the encoder side, OCaml strings are not required to be valid UTF-8:
validate and raise a typed error (a programmer bug, not recoverable), never
silently drop or re-encode.

## File structure

One file per concern; each maps to one layer.

```
foo/lib/
  sort.ml(i)     # Sort.t: closed enum of node categories
  value.ml(i)    # AST + Meta, pp/equal/queries, of_string shortcuts. No Codec dep.
  codec.ml(i)    # codec GADT, combinators, Codec.skip, identity codec, interpreters
  cursor.ml(i)   # zipper over Value.t (of_value / of_reader)
  stream.ml(i)   # optional: iter/fold helpers, transform
  error.ml(i)    # Foo.Error facade (see reference/errors.md)
  foo.ml(i)      # top-level: type aliases, IO entry points, re-exports
  dune
```

Separate `.ml` files with proper `.mli` boundaries avoid the mutual-recursion
gymnastics that one giant `foo.ml` with nested `module Value` / `module Codec`
forces, keep each file readable in one sitting, and let dune recompile only what
changed. [ocaml-json](https://github.com/samoht/ocaml-json) uses exactly this
layout.

## Packaging

**Flat in core; sub-libraries only for an externally-visible dep.**

```
foo        Core. Deps: bytesrw, uutf, uunf, ocaml-loc, fmt.
           Works on unix, mirage, js_of_ocaml.
foo.brr    Browser native-parse fast-path (JSON only, realistically).
```

Do not split for concerns (`foo.io`, `foo.parse`); that adds ceremony at call
sites with no portability benefit. Streaming is core via
`Bytesrw.Bytes.Reader.t` / `Bytesrw.Bytes.Writer.t`: pure OCaml, works
everywhere. Angstrom and Faraday are implementation choices, not required
public abstractions. When using them for a binary fast path, use their native
`Bigstringaf.t`/bigstring representation where the input and output pipeline
can preserve it; `Cstruct.t` is useful as a typed view, not as a replacement for
every byte sequence. Keep backend types and adapters private unless a separate,
deliberately documented binary IO API is part of the design. Do not claim
zero-copy across a `Bytesrw`/bigstring boundary without measuring the adapter
and preserving the relevant buffer lifetimes. Add any selected backend to the
package's declared dependencies; do not make an unused backend mandatory
merely because it is available.

**Mark internal helper modules `(private_modules ...)`.** Any `.ml` existing
only to share helpers between the public layers (the canonical case is a
`core.ml` of resizable arrays and byte helpers) belongs there:

```lisp
(library
 (name foo)
 (public_name foo)
 (private_modules core)
 (libraries bytesrw fmt (re_export loc)))
```

It enforces at the compiler level what the `.mli` doc already claims, and
silences merlint's `E605` (Missing Test File) correctly, rather than by adding a
meaningless test stub. In-library use is unaffected (`value.ml` can still
`open Core`), and a deliberate re-export still works (`module Rarray =
Core.Rarray` in `foo.ml` keeps `Foo.Rarray` public).

Rule of thumb: if the `.mli` header says "internal" or "not part of the public
surface", it is a `private_modules` candidate. Layer modules re-exported from
`foo.mli` (`Value`, `Codec`, `Cursor`, `Error`, `Binary`, `Tape`) are not
private; they are the deliberate public API.

## What not to expose

- **No independent lexeme / token stream.** Jsonm-style `Lexeme.t` +
  `Decoder.next` is a third abstraction beside Codec and Cursor. Streaming
  skip/filter/transform go through `Codec.skip`, `Cursor.of_reader`, and one
  byte-range primitive. Events and cursor moves are the same thing labelled by
  `Sort.t`.
- **No backend-specific public API.** Do not expose `Angstrom.t`, an
  `Angstrom.Unbuffered.state`, `Faraday.t`, `Bigstringaf.t`, or a second
  `Foo.angstrom` / `Foo.faraday` surface merely to reach the parser or writer.
  Drive private backends through the public Bytesrw reader/writer boundary.
  This does not forbid a separately designed binary fast-path API, but that
  API must make its bigstring ownership and lifetime contract explicit rather
  than leaking a parser backend accidentally.
- **No backwards-compat shims.** Restructure cleanly and bump the major version;
  don't keep `decode'` beside a new `decode`.
- **No format-buffet knobs.** Specs mandate UTF-8, so no
  ``?encoding:[`UTF8|`UTF16|`Latin1]``. Whitespace is the real choice and goes
  through `?indent` / `?preserve`.
- **No low-level machinery in the curated API.** `Base`, raw array maps,
  member-map builders and parser internals stay in the implementation `.mli` if
  siblings need them. Omit them from `foo.mli` unless you can say why an
  ordinary user should call them.

## Optional: a jq-like CLI

If the library has `Cursor`, ship a cram-tested CLI exposing it. It dogfoods the
API and doubles as usage documentation. Skip it for formats nobody queries
interactively (protobuf, BSON, MsgPack).

```
$ foo-cli '.users[0].email' < input.foo
"alice@example.com"
```

## Review checklist

Run `dune exec -- merlint <dir>` and treat findings as API review. Then confirm
the shape decisions that a build cannot catch:

- `Foo.t = Foo.Value.t`; `type 'a codec = 'a Codec.t` aliased in `foo.mli`.
- `Loc.Meta.t` on every `Value` constructor; identity codec at `Foo.Codec.Value.t`.
- `foo.mli` restates the public API; the odoc tree shows no `Codec.Base` /
  `Object.Member` / `Internal` subpages.
- Six top-level verbs, `_exn` twin for each `of_*` and `decode`, no `'` variants.
- Every `to_string` / writer encoder takes `?indent` + `?preserve`; every writer
  encoder makes `~eod:bool` explicit; decode metadata is one
  ``?meta:[`None | `Locs | `Full]`` knob.
- `Foo.Error` is a `Loc.Error` facade with typed (builder, raiser) pairs; no
  per-library exception, no sealed `kind` ADT.
- Hand-written parser failures use exceptions internally and wrap to `result`
  once at the boundary; incremental parser `Partial` is preserved as a driver
  state until more input arrives.
- `Bytesrw.Bytes.Reader.t` / `Bytesrw.Bytes.Writer.t` are used consistently;
  `of_reader` documents whether it consumes one document or all input, and
  `to_writer` makes `~eod:bool` explicit.
- Parser and serializer backend choices are private and benchmarked with
  realistic reader slice sizes, allocation counts, skip/filter cases, and
  output-buffer lifetimes.
- Flat core; sub-libraries only for external deps; helpers in `private_modules`.
- No lexeme-stream abstraction, no compat shims, no encoding knobs.
