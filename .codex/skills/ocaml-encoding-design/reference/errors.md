# The `Foo.Error` facade

`ocaml-loc` provides the canonical error machinery. Every encoding library ships
an `Error` submodule (`foo/lib/error.ml` + `.mli`) that is a **facade** over
`Loc.Error`, with four jobs:

1. Extend `Loc.Error.kind` with typed constructors for the format's recurring
   shape errors.
2. Register printers at module-init time so `Loc.Error.kind_to_string` stays
   total across extensions.
3. Re-export the `Loc.Error` surface so `Foo.Error.xxx` is the single place
   callers look.
4. Provide a typed builder + raiser pair for every recurring error shape.

## The builder / raiser convention

```ocaml
val foo      : Meta.t -> ... -> t      (* build, no raise *)
val fail_foo : Meta.t -> ... -> 'a     (* raise foo, same args *)
```

The pair lets callers accumulate errors (build) or abort (raise), and makes the
vocabulary uniform: everywhere in the codebase `fail_` means "this raises, same
args as the builder".

The primitive raiser `fail : t -> 'a` takes a pre-built error. `failf : Meta.t
-> fmt -> 'a` is the ad-hoc message raiser for cases not worth a typed
constructor; `msgf : Meta.t -> fmt -> t` is its non-raising twin.

## Worked shape

```ocaml
(* foo/lib/error.ml *)
module Sort = Sort

(* 1 -- alias re-opens the extensible variant under this module's name *)
type kind = Loc.Error.kind = ..

(* 2 -- typed constructors for this format *)
type Loc.Error.kind +=
  | Sort_mismatch of { exp : Sort.t; fnd : Sort.t }
  | Kinded_sort_mismatch of { exp : string; fnd : Sort.t }

(* 3 -- printers registered once, at module init *)
let () =
  Loc.Error.register_kind_printer @@ function
  | Sort_mismatch { exp; fnd } ->
    Some (fun ppf -> Fmt.pf ppf "Expected %a but found %a" Sort.pp exp Sort.pp fnd)
  | Kinded_sort_mismatch { exp; fnd } ->
    Some (fun ppf -> Fmt.pf ppf "Expected %a but found %a" Fmt.code exp Sort.pp fnd)
  | _ -> None

(* 4 -- re-exports and primitives *)
type t = Loc.Error.t = { ctx : Loc.Context.t; meta : Loc.Meta.t; kind : kind }

let v    = Loc.Error.v
let msg  = Loc.Error.msg
let fail = Loc.Error.raise   (* fail ~ctx ~meta kind : 'a *)

let msgf  meta fmt = Fmt.kstr (fun s -> msg ~ctx:Loc.Context.empty ~meta s) fmt
let failf meta fmt = Fmt.kstr (fun s -> Loc.Error.fail meta s) fmt

(* 5 -- typed errors, each a (builder, raiser) pair. The builder uses v/msgf to
   construct without raising; the raiser inlines the same body via fail/failf,
   one stack frame less than [fail (foo ...)]. *)

let sort meta ~exp ~fnd =
  v ~ctx:Loc.Context.empty ~meta (Sort_mismatch { exp; fnd })
let fail_sort meta ~exp ~fnd =
  fail ~ctx:Loc.Context.empty ~meta (Sort_mismatch { exp; fnd })

let expected meta exp ~fnd =
  msgf meta "Expected %a but found %a" Fmt.code exp Fmt.code fnd
let fail_expected meta exp ~fnd =
  failf meta "Expected %a but found %a" Fmt.code exp Fmt.code fnd

(* ... same pattern for kinded_sort, missing_members, unexpected_members,
   unexpected_case_tag, index_out_of_range, number_range, integer_range,
   no_decoder, no_encoder *)

(* Context pushers: pair (transform vs raise) *)
let push_array  sort n e = { e with ctx = Loc.Context.push_nth sort n e.ctx }
let push_object sort n e = { e with ctx = Loc.Context.push_mem sort n e.ctx }
let fail_push_array  = Loc.Error.push_array      (* t -> 'a *)
let fail_push_object = Loc.Error.push_object
```

`fail` takes `~ctx ~meta kind` directly, mirroring `v`'s arguments. A caller who
already holds a `t` and wants to re-raise uses `Stdlib.raise (Loc.Error e)`;
there is deliberately no `Error.raise_err : t -> 'a`, which would be a one-line
shadow of the standard exception primitive.

The `type kind = Loc.Error.kind = ..` alias is the pivot: it re-opens the
extensible variant under a local name, so users read `Foo.Error.kind` while the
underlying type is shared with every other format. The structural alias on `t`
means destructuring works against either module.

## Re-export in the top-level `.mli`

```ocaml
(* foo.mli *)
module Error = Error

exception Error of Error.t
(** Alias for [Loc.Error.Error]; exposed here so callers can [match] without
    importing [Loc]. *)
```

`error.mli` lists every primitive and every typed pair; `foo.mli` just
re-exports it.

## Recommended helper menu

Every format hits the same categories. A decoder written against this vocabulary
stays readable; one written against raw `failf` does not.

| Category | Helpers |
|----------|---------|
| Sort / shape mismatch | `sort`, `kinded_sort`, `expected` |
| Object members | `missing_mems`, `unexpected_mems`, `unexpected_case_tag` |
| Array / index | `index_out_of_range` |
| Number / integer range | `number_range`, `integer_range`, `parse_string_number` |
| Decoding direction | `no_decoder`, `decode_todo` |
| Encoding direction | `no_encoder`, `encode_todo` |
| Generic wrapper | `for'` |

`sort` / `kinded_sort` are mandatory: every format has a shape mismatch. Add the
rest as the decoder meets each category. Do not invent synonyms
(`wrong_member`, `bad_key`) when the menu already has the shape: decoder call
sites across formats should read the same.

## Banned in the facade

- `raise ~ctx ~meta kind` as a primitive. Fold into `fail (v ~ctx ~meta kind)`
  at call sites; two names for one thing confuses readers.
- `Error.pp_kind` / `pp_kind_opt` / `pp_int`: plain `Fmt` helpers, not
  error-specific. Keep them in the format's private `Core.Fmt`.
- `Error.disable_ansi_styler`: same reason.
- Helpers without a pair: if `foo` builds a `t`, there must be a `fail_foo`.
- Typed helpers whose body is a single `failf`. Inline them; named helpers are
  for shapes used in 3+ places with consistent wording.

## Anti-pattern: sealed `kind` + own exception

```ocaml
(* BAD *)
type kind = Lexer of lexer_error | Number of number_error | Syntax of syntax_error
type t = { kind : kind; location : location option }
exception Error of t
```

This duplicates `Loc.Error` (two exception types to catch, two context
structures, two printer pipelines), forecloses third-party extension (downstream
cannot add a kind without patching `foo/lib/error.ml`), and invents a parallel
`location` record instead of reusing `Loc.Meta.t` + `Loc.Context.t`. Every
format eventually wants a domain-specific error from a validation or schema
layer; the extensible `Loc.Error.kind` accommodates that, a sealed ADT does not.

## Invariants

- Errors use `Loc.Error.t` and `Loc.Error.Error`, one result type and one
  exception across every format.
- Return `(_, Loc.Error.t) result`; a caller who wants a string does
  `Result.map_error Loc.Error.to_string`.
- `Loc.Context.push_array` / `push_object` chain during parsing, so errors
  report `users[0].email` paths for free.
- Never catch and re-raise `Loc.Error.Error` just to reshape its kind. If a call
  site needs a different label, raise the right typed kind at the source.

[ocaml-json/lib/error.ml](https://github.com/samoht/ocaml-json/blob/main/lib/error.ml)
is the worked exemplar.

## `Loc.Context.t` and extensible path steps

The context (path + source loc + active sort) is first-class, and like
`Loc.Error.kind` the path-step type is **extensible**: `Mem` and `Nth` cover the
common case, formats add their own when the baseline is lossy.

```ocaml
module Loc : sig
  module Path : sig
    type step = ..                 (* extensible *)
    type step += Mem of string     (* name-addressed, baseline *)
    type step += Nth of int        (* index-addressed, baseline *)
    val register_step_printer :
      (step -> (Format.formatter -> unit) option) -> unit

    type t                          (* root-to-leaf *)
    val empty : t
    val push  : step -> t -> t
    val last  : t -> step option
    val to_list : t -> step list
  end

  module Context : sig
    type t
    val path : t -> Path.t
    val loc  : t -> loc
    val sort : t -> Sort.t
    val push : Path.step -> t -> t
  end
end
```

Per-format extensions declare native addressing:

```ocaml
(* XML -- attributes are not named children, namespaces matter *)
type Loc.Path.step +=
  | Attribute  of string
  | Namespaced of { ns : string; local : string }

(* CBOR / MsgPack -- map keys are arbitrary values, not just strings *)
type Loc.Path.step += Cbor_key of Cbor.Value.t

(* protobuf -- wire addressing is by field number *)
type Loc.Path.step += Field_number of int
```

Tooling that only knows `Mem` / `Nth` (diff viewers, JSON Pointer emitters)
works everywhere on the baseline; format-aware tooling reads native steps when
present. The printer registry keeps `Loc.Path.pp` total.

Everyone speaks the same `Loc.Context.t`: errors hold one, cursors produce one
(`Cursor.to_context` / `of_context`), streams hand one to callbacks
(`Stream.fold ~f:(fun ctx ...)`). One noun, top to bottom.

If `ocaml-loc` lacks something, extend `ocaml-loc`; don't fork.
