# Streaming: Codec and Cursor are dual views

A materialized cursor (zipper over `Value.t`) is right when the document fits in
memory. For multi-GB logs, event streams, or edit-one-field-keep-the-rest, you
need streaming. The design question is what goes in `Codec`, what goes in
`Cursor`, and what needs new primitives below both.

## Summary: what goes where

| Use case | Lives in |
|----------|----------|
| Skip an element class | `Foo.Codec.skip` |
| Filter by element type | codec combinators (`list (either ...)`) |
| Fold/iter over children | `Foo.Stream.fold` / `iter` |
| Decode a subtree after filtering | `Foo.Codec.decode` |
| Random-access navigation, edit | `Foo.Cursor.of_value` |
| Forward-only navigation over bytes | `Foo.Cursor.of_reader` |
| Layout-preserving transform | `Foo.Stream.transform` (byte-range) |
| Incremental input parsing | private scanner or Angstrom driver |
| Buffered output serialization | direct writer or private Faraday adapter |

Only the transform path needs primitives below codec: byte offset (`tell`),
byte-range splice (`splice_to`), and raw event peek. The rest is codec + cursor
composition.

## Parser and serializer backends

The public streaming boundary is `Bytesrw.Bytes.Reader.t` /
`Bytesrw.Bytes.Writer.t`. The parser and serializer underneath it are private
implementation choices; neither Angstrom nor Faraday belongs in `Foo.Codec`.

For input, use a hand-written scanner when format-specific control, source
locations, or byte-range preservation dominate. Consider
[Angstrom](https://ocaml.org/p/angstrom/0.16.1/doc/angstrom/Angstrom/index.html)
for incremental combinator parsing, especially when the format is consumed in
chunks or from a non-blocking driver. Start with `Angstrom.Buffered`; use
`Angstrom.Unbuffered` only when measured allocation or copying costs justify
its more involved input-management contract. `Partial` means that the driver
must provide more input, not that the format is invalid. Map terminal parser
failures into `Loc.Error` and maintain absolute offsets and `Loc.Context` in the
format adapter rather than relying on parser error strings.

For output, consider
[Faraday](https://ocaml.org/p/faraday/0.8.2) when serialization needs a
reusable buffer, queued output, or vectorized writes. Buffered `write_*`
operations copy into Faraday's buffer; `schedule_*` operations borrow their
source strings or bigstrings until the queued output is drained. A Faraday
adapter must honor those lifetimes while draining into the Bytesrw writer and
must not write the stream end marker until the caller's explicit `~eod:bool`
policy permits it.

Angstrom's zero-copy input path uses `Bigstringaf.t`, and Faraday's `bigstring`
operations use the same byte `Bigarray.Array1.t` with C layout. Use
`Bigstringaf.t`/bigstring as the precise term for this ecosystem path:
`Bigarray` is the general storage abstraction, while
[Cstruct](https://ocaml.org/p/cstruct/latest/doc/cstruct/Cstruct/index.html)
is an optional offset/length view with binary-field accessors. For binary or
protocol codecs, prefer bigstrings when the surrounding IO already provides
them; use Cstruct when its typed views improve the implementation. Neither is
automatically faster for every workload.

Bytesrw's ordinary byte slices are backed by `Bytes.t`, so crossing the
Bytesrw/bigstring boundary may copy. Treat zero-copy as a benchmarked property
of a particular adapter, including buffer retention and mutation lifetimes,
not as an automatic consequence of choosing Angstrom or Faraday. Keep the
public Bytesrw boundary unless a separate, deliberately documented bigstring
IO API is justified by the format and workload.

Do not run a full-`Value.t` Angstrom parser and then interpret the tree when a
typed codec can skip or decode directly. The backend must preserve the same
skip, path, source-location, and bounded-memory guarantees as the hand-written
parse kernel.

## Skip and filter are codec-layer

`Codec.skip` is inherently a codec value (`unit Codec.t`): the decoder
reads-and-discards, the encoder emits an empty element. It belongs in `Codec`,
not at top level (`Foo.skip` is too generic without the namespace) and not
duplicated into `Stream`.

```ocaml
(* In codec.mli *)
val skip : unit t
```

Filtering then falls out of existing combinators:

- `list Codec.skip` consumes a list of uninteresting elements.
- `El.child_opt "trace" Codec.skip` drops a known-uninteresting child inside a
  record codec.
- `list (either interesting skip)` processes only matching elements.

The implementation rides on `Codec_fn` + `stream_skip_*` helpers in the parser;
no new primitive. All skip/filter workflows work codec-only, even over gigabyte
documents, as long as each *kept* sub-element fits in memory.

## Stream events are cursor moves, labelled by Sort

A pull-parser event and a cursor move are the same thing at different
granularities:

| Pull-parser event | Cursor move | Sort label |
|-------------------|-------------|------------|
| `Start_element tag` | `down_field tag` (descend) | `Sort.Element` |
| `End_element tag` | `up` | `Sort.Element` |
| `Text s` | visit-leaf (no descent) | `Sort.Text` |
| `Attribute (name, val)` | attribute leaf on current elt | `Sort.Attribute` |

The same `Sort.t` that labels `Loc.Context` frames labels stream events. One
vocabulary, top to bottom, which is why a public jsonm-style lexeme API would
be a redundant third surface.

## Cursor has two constructors

```ocaml
module Cursor : sig
  type t
  val of_value  : Value.t -> t            (* random-access zipper *)
  val of_reader : Bytesrw.Bytes.Reader.t -> t (* forward-only, one-pass *)

  (* Same navigation API, different guarantees per backend. *)
  val focus        : t -> Value.t         (* materializes current subtree *)
  val descend      : string -> t -> t option
  val next_sibling : t -> t option
  val up           : t -> t option        (* may be None on stream backend *)
  val skip         : t -> t option        (* always works forward *)
end
```

Both backends share `Sort.t` labels, `Loc.Path.t` position reporting, and the
same `Codec` entry points. The forward-only backend trades random access for
bounded memory: only the current subtree is in RAM at any moment.

`focus` on the stream backend is destructive; it reads through the matching end
tag, leaving the stream at the next sibling. Document that on the function.

## `Foo.Stream`

Operations that are not codecs but compose *with* codecs live in `Foo.Stream`,
its own `stream.ml` / `stream.mli`.

`fold` and `iter` visit every child of the current parent. The callback receives
a `Loc.Context.t`: the same context `Cursor.to_context` produces and the same
one a decode error carries, bundling path, source loc, and active sort.

```ocaml
module Stream : sig
  (* Iterate every child, applying the codec per visit. Match on
     [Context.path] / [Context.last_step] to branch, read [Context.loc] for
     source position, forward into [Loc.Error.v ~ctx] to raise. Memory is
     bounded by one child at a time; when the codec does not apply (e.g.
     [Codec.skip]), the child is skipped without materialization. *)
  val fold :
    'child Codec.t ->
    f:(Loc.Context.t -> 'acc -> 'child -> 'acc) ->
    init:'acc ->
    Bytesrw.Bytes.Reader.t -> 'acc

  val iter :
    'child Codec.t ->
    f:(Loc.Context.t -> 'child -> unit) ->
    Bytesrw.Bytes.Reader.t -> unit

  (* Layout-preserving transform: byte-copy untouched regions, materialize and
     re-serialize only the sub-trees the user edits or drops. Preserves
     comments, whitespace, attribute order, and entity escape choice in the
     untouched 99%. *)
  val transform :
    Bytesrw.Bytes.Reader.t -> Bytesrw.Bytes.Writer.t -> eod:bool ->
    f:(Loc.Context.t -> [ `Copy | `Edit of (Value.t -> Value.t) | `Drop ]) ->
    unit
end
```

Filtering happens inside `f`, by matching on the context's path:

```ocaml
(* Count <user> children of a parent element *)
let count_users reader =
  Stream.fold Xml.Codec.skip reader ~init:0
    ~f:(fun ctx acc () -> match Loc.Context.last_step ctx with
      | Some (Mem "user") -> acc + 1
      | _ -> acc)

(* Sum the [amount] of every transaction under [/orders/*] --
   outer context matters, so match the whole path *)
let sum_amounts reader =
  Stream.fold (Codec.mem "amount" Codec.float) reader ~init:0.0
    ~f:(fun ctx acc x -> match Loc.Context.path ctx with
      | [Mem "orders"; Nth _; Mem "amount"] -> acc +. x
      | _ -> acc)

(* Raise with the stream's own context for a domain-specific error *)
let require_positive reader =
  Stream.iter Codec.int reader
    ~f:(fun ctx n ->
      if n <= 0 then Loc.Error.raise (Loc.Error.v ~ctx (`Non_positive n)))
```

All three helpers stamp the same context into decoder errors, so a failure
reports the exact `Context.t` the callback saw for that child.

## `Stream.transform` needs one byte-range primitive

"Edit one field, keep the rest byte-identical" (a 10 GB log, change one
attribute, preserve comments and whitespace in the other 99.99%) is not
expressible in pure codec or pure cursor:

- A codec `Value.t <-> bytes` re-serializes on encode: whitespace canonicalized,
  comments dropped, attribute order potentially shifted.
- A cursor over `Value.t` has no access to raw bytes.

One primitive below the codec layer closes the gap:

```ocaml
(* In the internal P module *)
val P.tell : stream -> int
val P.splice_to : stream -> Bytesrw.Bytes.Writer.t -> stop:int -> unit
  (* copy bytes from last-spliced position up to [stop] into [writer] *)
```

`Stream.transform` then becomes codec + cursor composition: read events; for
`Copy` splice untouched bytes through; for `Edit` materialize the subtree via
the identity codec, apply the function, serialize the result; for `Drop`
skip past via `P.skip_element`. Untouched bytes reach the writer byte-identical.
The transform must document whether it consumes one document or all input and
must write `Bytesrw.Bytes.Slice.eod` exactly when `~eod:true`.
