---
name: ocaml-rfcs
description: Working with IETF RFCs (Request for Comments). Fetch, document, and integrate RFC specifications into OCaml codebases with proper citations and cross-references. Use this skill when you need to - document RFC compliance, validate against an RFC, cite RFCs in documentation, check RFC requirements, fetch an RFC specification.
license: ISC
metadata:
    copyright: Copyright (c) 2025 Anil Madhavapejddy <anil@recoil.org>
---

# OCaml RFCs

## RFC Fetching and Storage

### Fetching RFCs

Always fetch RFCs in **plain text format** from: <https://datatracker.ietf.org/doc/html/rfcXXXX.txt>

**IMPORTANT**: Use the `.txt` extension, not `.html`. For example:

- do: `https://datatracker.ietf.org/doc/html/rfc6265.txt`
- dont: `https://datatracker.ietf.org/doc/html/rfc6265.html`

### Storage Location

**ALWAYS** save fetched RFC files to the `spec/` directory in the project root:

```txt
spec/rfc6265.txt
spec/rfc3492.txt
spec/rfc5891.txt
```

If the `spec/` directory doesn't exist, create it first.

### Workflow for New RFCs

When a new RFC is mentioned or needed:

1. Check if `spec/rfcXXXX.txt` already exists
2. If not, fetch it using WebFetch tool
3. Save the content to `spec/rfcXXXX.txt` using Write tool
4. Confirm the file was saved successfully

---

## OCamldoc RFC Citation Format

### Basic RFC Link

OCamldoc uses this format for external links:

```ocaml
{{:https://datatracker.ietf.org/doc/html/rfcXXXX}RFC XXXX}
```

Example:

```ocaml
(** Implementation of {{:https://datatracker.ietf.org/doc/html/rfc3492}RFC 3492}
    Punycode encoding. *)
```

### Section-Specific Links

For linking to specific sections, use the anchor format:

```ocaml
{{:https://datatracker.ietf.org/doc/html/rfcXXXX#section-N.M}RFC XXXX Section N.M}
```

Example:

```ocaml
(** Implements the bias adaptation algorithm from
    {{:https://datatracker.ietf.org/doc/html/rfc3492#section-6.1}RFC 3492 Section 6.1}. *)
```

### Common Section References

- `#section-N` - Main numbered section
- `#section-N.M` - Subsection
- `#appendix-X` - Appendix (A, B, C, etc.)
- `#page-N` - Specific page reference (use sparingly)

### Multiple References

When a function implements multiple RFC sections, list them clearly:

```ocaml
(** [encode codepoints] encodes Unicode code points to Punycode ASCII.

    Implements the encoding procedure from
    {{:https://datatracker.ietf.org/doc/html/rfc3492#section-6.3}RFC 3492 Section 6.3}:

    {ul
    {- Basic code point segregation per
       {{:https://datatracker.ietf.org/doc/html/rfc3492#section-3.1}Section 3.1}}
    {- Variable-length integer encoding per
       {{:https://datatracker.ietf.org/doc/html/rfc3492#section-3.3}Section 3.3}}
    {- Overflow handling per
       {{:https://datatracker.ietf.org/doc/html/rfc3492#section-6.4}Section 6.4}}} *)
```

---

## Linking Code to RFC Sections

### Module-Level Documentation

At the top of modules, provide:

- Main RFC reference with full title
- Brief description of what the RFC specifies
- Links to key related RFCs

Example:

```ocaml
(** RFC 3492 Punycode: A Bootstring encoding of Unicode for IDNA.

    This module implements the Punycode algorithm as specified in
    {{:https://datatracker.ietf.org/doc/html/rfc3492}RFC 3492},
    providing encoding and decoding of Unicode strings to/from ASCII-compatible
    encoding suitable for use in internationalized domain names.

    {2 References}
    {ul
    {- {{:https://datatracker.ietf.org/doc/html/rfc3492}RFC 3492} - Punycode: A Bootstring encoding of Unicode for IDNA}
    {- {{:https://datatracker.ietf.org/doc/html/rfc5891}RFC 5891} - IDNA Protocol}} *)
```

### Function-Level Documentation

Link each function to its specific RFC section:

```ocaml
val adapt : delta:int -> numpoints:int -> firsttime:bool -> int
(** [adapt ~delta ~numpoints ~firsttime] computes the new bias value.

    Implements the bias adaptation algorithm from
    {{:https://datatracker.ietf.org/doc/html/rfc3492#section-6.1}RFC 3492 Section 6.1}.

    @param delta The delta value that was just encoded
    @param numpoints Number of code points processed so far
    @param firsttime Whether this is the first delta *)
```

### Type-Level Documentation

For types representing RFC concepts:

```ocaml
type error =
  | Overflow of position
      (** Arithmetic overflow during encode/decode. This can occur with
          very long strings or extreme Unicode code point values.
          See {{:https://datatracker.ietf.org/doc/html/rfc3492#section-6.4}
          RFC 3492 Section 6.4} for overflow handling requirements. *)
  | Invalid_digit of position * char
      (** An invalid Punycode digit was encountered during decoding.
          Valid digits are a-z, A-Z (values 0-25) and 0-9 (values 26-35).
          See {{:https://datatracker.ietf.org/doc/html/rfc3492#section-5}
          RFC 3492 Section 5} for digit-value mappings. *)
```

### Constants and Parameters

Document RFC parameter values:

```ocaml
val base : int
(** The base value (36) for Punycode variable-length integer encoding.
    See {{:https://datatracker.ietf.org/doc/html/rfc3492#section-5}
    RFC 3492 Section 5} for Bootstring parameters. *)

val ace_prefix : string
(** The ACE prefix ["xn--"] used for Punycode-encoded domain labels.
    See {{:https://datatracker.ietf.org/doc/html/rfc3492#section-5}
    RFC 3492 Section 5} which notes that IDNA prepends this prefix. *)
```

---

## Reading and Parsing RFC Files

### RFC Structure

When reading RFC text files, understand the common structure:

1. **Header**: RFC number, title, authors, date
2. **Table of Contents**: Section numbers and titles
3. **Abstract**: Brief summary
4. **Main Body**: Numbered sections
5. **Appendices**: Lettered sections (A, B, C, etc.)
6. **References**: Citations to other documents

### Extracting Section Content

When asked about a specific section:

1. Search for the section number (e.g., "6.1" or "6.1.")
2. Extract content until the next section header
3. Include any subsections
4. Note any references to other sections

### Common Section Titles to Look For

- **Introduction** - Background and motivation
- **Terminology** - Key terms and definitions
- **Requirements** / **Notation** - Conventions used
- **Algorithm** / **Procedure** - Core specification
- **Security Considerations** - Security implications
- **IANA Considerations** - Registry considerations
- **References** - Normative and informative references

---

## Code Analysis and Suggestions

### Finding Missing RFC References

When analyzing code that implements an RFC:

1. **Read the RFC file** from `spec/` directory
2. **Identify key algorithms and procedures** in the RFC
3. **Search the codebase** for functions that implement these
4. **Check if functions have RFC citations** in their documentation
5. **Suggest additions** where citations are missing

Example suggestions:

```txt
Found function `adapt` at line 123 that appears to implement the
bias adaptation algorithm. Consider adding a reference:

    {{:https://datatracker.ietf.org/doc/html/rfc3492#section-6.1}RFC 3492 Section 6.1}
```

### Validating Existing References

Check that:

- RFC numbers are correct
- Section numbers exist in the RFC
- URLs are properly formatted
- Citations match the actual implementation

### Consistency Checks

Ensure:

- All functions implementing the same RFC use consistent citation format
- Related functions reference the appropriate sections
- Error types cite relevant RFC requirements
- Constants reference their RFC definitions

### Suggesting Improvements

Look for opportunities to:

- Add RFC context to bare function names
- Link error conditions to RFC requirements
- Document why certain checks are necessary per the RFC
- Add "See also" references to related RFC sections

---

## Example Workflows

### Workflow 1: Adding RFC Support to New Module

1. User mentions implementing RFC 6265 (HTTP Cookies)
2. Check if spec/rfc6265.txt exists
3. If not: fetch from <https://datatracker.ietf.org/doc/html/rfc6265.txt>
4. Save to spec/rfc6265.txt
5. Read the RFC to understand structure
6. Add module-level documentation with RFC reference
7. Identify key sections (parsing, validation, security)
8. Document functions with section-specific references

### Workflow 2: Improving Existing RFC Documentation

1. User asks to improve RFC documentation in module
2. Read the .mli file to see current state
3. Read spec/rfcXXXX.txt to understand specification
4. Identify functions missing RFC references
5. Find correct section numbers for each function
6. Suggest specific documentation improvements
7. Apply changes if user approves

### Workflow 3: Validating RFC Implementation

1. User asks to validate RFC implementation
2. Read spec/rfcXXXX.txt
3. Extract key requirements (MUST, SHOULD, MAY)
4. Read implementation code
5. Check each requirement is implemented
6. Verify error handling matches RFC
7. Report gaps or inconsistencies

---

## Best Practices

### DO

✅ Always fetch and save RFC text files to `spec/`
✅ Use section-specific links when possible
✅ Link error types to their RFC requirements
✅ Document RFC parameters and constants
✅ Keep citations consistent across related functions
✅ Reference the "why" from the RFC, not just "what"
✅ Include RFC context in type documentation

### DON'T

❌ Don't link to HTML versions of RFCs
❌ Don't assume RFC sections without checking
❌ Don't omit section numbers in citations
❌ Don't duplicate RFC text verbatim (summarize)
❌ Don't forget to save fetched RFCs
❌ Don't cite non-existent RFC sections

---

## OCamldoc Formatting Quick Reference

```ocaml
(** Single RFC link: {{:URL}RFC XXXX} *)

(** RFC with section: {{:URL#section-N}RFC XXXX Section N} *)

(** Bulleted list of references:
    {ul
    {- {{:URL1}RFC 1234}}
    {- {{:URL2}RFC 5678}}} *)

(** Inline reference in text: Per {{:URL}RFC 3492 Section 5}, the base is 36. *)

(** Parameter with RFC:
    @param delta The delta value (see {{:URL}RFC 3492 Section 6.3}) *)

(** Error with RFC requirement:
    @raise Invalid_argument if input violates {{:URL}RFC 3492 Section 3.1} *)
```

---

## Error Handling

When you encounter:

- **RFC not found** (404): Verify RFC number exists, may be a draft or obsolete
- **Section doesn't exist**: Re-read RFC, section numbering may have changed
- **Can't save to spec/**: Check directory permissions, create if needed
- **Malformed RFC text**: Some RFCs have encoding issues, fetch again

---

## Advanced Features

### Handling RFC Updates and Obsoletes

When an RFC obsoletes another:

```ocaml
(** Implements {{:https://datatracker.ietf.org/doc/html/rfc6265}RFC 6265}
    (HTTP State Management), which obsoletes
    {{:https://datatracker.ietf.org/doc/html/rfc2965}RFC 2965}. *)
```

### Cross-Referencing Multiple RFCs

When implementing functionality spanning multiple RFCs:

```ocaml
(** IDNA-compatible domain name encoding.

    Combines {{:https://datatracker.ietf.org/doc/html/rfc5891}RFC 5891}
    (IDNA Protocol) with {{:https://datatracker.ietf.org/doc/html/rfc3492}RFC 3492}
    (Punycode) for internationalized domain names. *)
```

### Noting RFC Errata

If there are known errata:

```ocaml
(** Note: This implementation follows the errata for
    {{:https://datatracker.ietf.org/doc/html/rfc3492}RFC 3492},
    which clarifies the overflow handling in Section 6.4. *)
```

---

## Review Checklist

When working with RFCs, ensure you:

- [ ] Fetch RFC in plain text format (`.txt`)
- [ ] Save to `spec/rfcXXXX.txt`
- [ ] Add module-level RFC reference with title
- [ ] Link functions to specific RFC sections
- [ ] Document RFC parameters and constants
- [ ] Link error types to RFC requirements
- [ ] Use consistent citation format throughout
- [ ] Validate all section numbers exist
- [ ] Suggest improvements for existing code
- [ ] Check for missing RFC references
