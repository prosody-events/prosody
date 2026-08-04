//! Diagnostic pins for both macros.
//!
//! Every fixture is parsed from a source string (`syn::parse_str`), never
//! `parse_quote!`: minted tokens all carry the call site, so only a real parse
//! gives the `syn::Error` a line and column to assert. `proc-macro2`'s
//! `span-locations` feature (a dev-dependency here) is what makes those
//! locations readable.
//!
//! Each test pins the *whole* diagnostic list — every message and the exact
//! token it covers — because a correct message on the wrong token is still a
//! bad diagnostic, and a spurious extra rejection is a regression the count
//! catches. Tests here return `syn::Error` rather than `color_eyre::Result`:
//! a proc-macro crate carries no error-reporting dependency, and the rejection
//! is already the value under test.

use crate::methods::{Args, rewrite};
use crate::{layout, methods};
use proc_macro2::{Span, TokenStream};
use quote::ToTokens;
use syn::{Error, ItemImpl, parse_str};

/// One expected diagnostic: its message and the `(line, column)` range it
/// covers. Columns are zero-based, lines one-based, as `proc-macro2` reports
/// them.
type Expected<'a> = (&'a str, (usize, usize, usize, usize));

/// The attribute arguments every accepted fixture uses.
const ARGS: &str = "field = cells, session = S";

/// The `self`-ban message, shared by the fixtures that trip it.
const SELF_MESSAGE: &str = "a marked collection method body may not reference `self`; use `op`, a \
                            method argument, or a free helper taking `&mut impl CollectionRead`";

/// The `op`-collision message for a method argument.
const OP_ARGUMENT_MESSAGE: &str =
    "`op` names the scoped operation inside this body; rename the argument";

/// The message every non-`async` marked method trips.
const NON_ASYNC_MESSAGE: &str = "a marked collection method is `async`: it acquires admission \
                                 once per invocation, and no marked method streams";

/// Rewrites one `impl` fixture and returns the rejection it produced, if any.
/// An unparsable argument list is itself a rejection, not a test failure.
fn diagnose_methods(args: &str, source: &str) -> Result<Option<Error>, Error> {
    let mut item: ItemImpl = parse_str(source)?;
    match parse_str::<Args>(args) {
        Ok(args) => Ok(rewrite(&mut item, &args)),
        Err(error) => Ok(Some(error)),
    }
}

/// Rewrites one `impl` fixture that must be accepted, returning its expansion.
fn expand_methods(args: &str, source: &str) -> Result<TokenStream, Error> {
    let args: Args = parse_str(args)?;
    let mut item: ItemImpl = parse_str(source)?;
    match rewrite(&mut item, &args) {
        Some(error) => Err(error),
        None => Ok(item.into_token_stream()),
    }
}

/// Expands one `collection_layout!` fixture and returns the rejection it
/// produced, if any.
fn diagnose_layout(source: &str) -> Result<Option<Error>, Error> {
    let tokens: TokenStream = parse_str(source)?;
    Ok(layout::expand(tokens).err())
}

/// Asserts the full list of rejections a fixture produced, in order.
#[track_caller]
fn assert_diagnostics(error: Option<Error>, expected: &[Expected<'_>]) -> Result<(), Error> {
    let Some(error) = error else {
        return Err(Error::new(
            Span::call_site(),
            "expected a rejection, got an accepted expansion",
        ));
    };
    let found: Vec<(String, (usize, usize, usize, usize))> = error
        .into_iter()
        .map(|one| {
            let located = one.span();
            let (start, end) = (located.start(), located.end());
            (
                one.to_string(),
                (start.line, start.column, end.line, end.column),
            )
        })
        .collect();
    let expected: Vec<(String, (usize, usize, usize, usize))> = expected
        .iter()
        .map(|&(message, span)| (message.to_owned(), span))
        .collect();
    assert_eq!(
        found, expected,
        "every diagnostic must match its message and its responsible token"
    );
    Ok(())
}

const SELF_IN_TAIL: &str = "\
impl Handle {
    #[read(op)]
    async fn bad(&self) -> Result<u32, HandleError> {
        self.other().await
    }
}
";

const SELF_NESTED: &str = "\
impl Handle {
    #[write(op)]
    async fn bad(&self, n: u32) -> Result<u32, HandleError> {
        let total: u32 = (0..n)
            .map(|i| i + helper(op, self.base))
            .sum();
        Ok(total)
    }
}
";

const SELF_IN_MACRO: &str = "\
impl Handle {
    #[read(op)]
    async fn bad(&self) -> Result<u32, HandleError> {
        let (a, b) = tokio::join!(self.get(), other(op));
        Ok(a + b)
    }
}
";

const NO_SELF: &str = "\
impl Handle {
    #[read(op)]
    async fn good(&self, n: u32) -> Result<u32, HandleError> {
        fn scale(op: u32) -> u32 { op * 2 }
        let window = self::bounds(op).await?;
        Ok(scale(window) + n)
    }

    fn unmarked(&self) -> u32 {
        self.cells.len()
    }
}
";

const MUT_SELF: &str = "\
impl Handle {
    #[read(op)]
    async fn bad(&mut self) -> u32 {
        0
    }
}
";

const BY_VALUE_SELF: &str = "\
impl Handle {
    #[read(op)]
    async fn bad(self) -> u32 {
        0
    }
}
";

const BAD_RECEIVERS: &str = "\
impl Handle {
    #[read(op)]
    async fn first(n: u32) -> u32 {
        n
    }

    #[read(op)]
    async fn none() -> u32 {
        0
    }
}
";

const NON_ASYNC_WRITE: &str = "\
impl Handle {
    #[write(op)]
    fn bad(&self) -> u32 {
        0
    }
}
";

const NON_ASYNC_READ: &str = "\
impl Handle {
    #[read(op)]
    fn items(&self) -> impl Stream<Item = u32> + '_ {
        op.coordinates(0)
    }
}
";

const OP_ARGUMENT: &str = "\
impl Handle {
    #[read(op)]
    async fn bad(&self, op: u32) -> u32 {
        0
    }
}
";

const OP_DESTRUCTURED: &str = "\
impl Handle {
    #[read(op)]
    async fn bad(&self, (op, n): (u32, u32)) -> u32 {
        0
    }
}
";

const OP_BINDING: &str = "\
impl Handle {
    #[read(op)]
    async fn bad(&self) -> u32 {
        let op = 1;
        op
    }
}
";

const DUPLICATE_MARKER: &str = "\
impl Handle {
    #[read(op)]
    #[write(op)]
    async fn bad(&self) -> u32 {
        0
    }
}
";

const MALFORMED_RESOLVE: &str = "\
impl Handle {
    #[read(op, into(T))]
    async fn bad(&self) -> u32 {
        0
    }
}
";

#[test]
fn self_in_marked_body_reported_at_self_token() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_methods(ARGS, SELF_IN_TAIL)?,
        &[(SELF_MESSAGE, (4, 8, 4, 12))],
    )
}

#[test]
fn nested_self_reported_at_self_token() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_methods(ARGS, SELF_NESTED)?,
        &[(SELF_MESSAGE, (5, 36, 5, 40))],
    )
}

/// A macro's arguments are unparsed tokens, so the visitor cannot reach the
/// `self` a `join!` would concurrently re-enter through.
#[test]
fn self_inside_a_macro_invocation_rejected() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_methods(ARGS, SELF_IN_MACRO)?,
        &[(SELF_MESSAGE, (4, 34, 4, 38))],
    )
}

/// `self::` is a module path, an item nested in the body captures neither the
/// receiver nor the operation, and unmarked methods are untouched.
#[test]
fn module_paths_nested_items_and_unmarked_methods_accepted() -> Result<(), Error> {
    let rendered = expand_methods(ARGS, NO_SELF)?.to_string();
    assert!(
        rendered.contains("cells . read"),
        "the marked method must run in a read scope: {rendered}"
    );
    assert!(
        rendered.contains("self :: bounds (op)"),
        "a `self::` module path is not a receiver reference: {rendered}"
    );
    assert!(
        rendered.contains("fn scale (op : u32)"),
        "a nested item's own `op` is not a shadow: {rendered}"
    );
    assert!(
        rendered.contains("self . cells . len"),
        "an unmarked method must be copied through untouched: {rendered}"
    );
    Ok(())
}

#[test]
fn mut_self_receiver_rejected() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_methods(ARGS, MUT_SELF)?,
        &[(
            "a marked collection method takes `&self`; admission is acquired per invocation, so \
             the handle is never mutated",
            (3, 22, 3, 26),
        )],
    )
}

#[test]
fn by_value_receiver_rejected() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_methods(ARGS, BY_VALUE_SELF)?,
        &[(
            "a marked collection method takes `&self`; admission is acquired per invocation, so \
             the handle is never consumed",
            (3, 17, 3, 21),
        )],
    )
}

#[test]
fn a_missing_receiver_is_rejected_at_the_offending_token() -> Result<(), Error> {
    const MESSAGE: &str = "a marked collection method takes `&self`";
    assert_diagnostics(
        diagnose_methods(ARGS, BAD_RECEIVERS)?,
        &[(MESSAGE, (3, 19, 3, 25)), (MESSAGE, (8, 13, 8, 17))],
    )
}

#[test]
fn non_async_write_rejected() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_methods(ARGS, NON_ASYNC_WRITE)?,
        &[(NON_ASYNC_MESSAGE, (2, 6, 2, 11))],
    )
}

/// There is no stream lowering: a non-`async` read is rejected at its marker
/// rather than silently wrapped.
#[test]
fn non_async_read_rejected() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_methods(ARGS, NON_ASYNC_READ)?,
        &[(NON_ASYNC_MESSAGE, (2, 6, 2, 10))],
    )
}

#[test]
fn op_argument_collision_rejected() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_methods(ARGS, OP_ARGUMENT)?,
        &[(OP_ARGUMENT_MESSAGE, (3, 24, 3, 26))],
    )
}

/// A destructured argument binds `op` just as a plain one does.
#[test]
fn destructured_op_argument_collision_rejected() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_methods(ARGS, OP_DESTRUCTURED)?,
        &[(OP_ARGUMENT_MESSAGE, (3, 25, 3, 27))],
    )
}

#[test]
fn op_binding_shadow_rejected() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_methods(ARGS, OP_BINDING)?,
        &[(
            "`op` names the scoped operation inside this body; rename the binding",
            (4, 12, 4, 14),
        )],
    )
}

#[test]
fn two_markers_on_one_method_rejected() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_methods(ARGS, DUPLICATE_MARKER)?,
        &[(
            "a method runs in exactly one scope; `op` is already bound",
            (3, 6, 3, 11),
        )],
    )
}

#[test]
fn malformed_marker_argument_rejected() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_methods(ARGS, MALFORMED_RESOLVE)?,
        &[("expected `resolve(<type>)`", (2, 15, 2, 19))],
    )
}

#[test]
fn missing_session_argument_rejected() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_methods("field = cells", NON_ASYNC_WRITE)?,
        &[(
            "`#[collection_methods]` needs `session = <ident>` naming the impl's session type \
             parameter; the write and resolver bounds on marked methods are attached to it",
            (1, 0, 1, 5),
        )],
    )
}

#[test]
fn missing_field_argument_rejected() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_methods("session = S", NO_SELF)?,
        &[(
            "`#[collection_methods]` needs `field = <ident>` naming the handle field that holds \
             the bound collection",
            (1, 0, 1, 7),
        )],
    )
}

#[test]
fn unknown_argument_key_rejected() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_methods("collection = cells", NO_SELF)?,
        &[(
            "expected `field = <ident>` or `session = <ident>`",
            (1, 0, 1, 10),
        )],
    )
}

#[test]
fn repeated_argument_key_rejected_at_the_second_key() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_methods("field = cells, field = other, session = S", NO_SELF)?,
        &[(
            "`field` is given twice; each argument appears once",
            (1, 15, 1, 20),
        )],
    )
}

#[test]
fn resolver_bound_inferred_from_the_return_type() -> Result<(), Error> {
    const RESOLVING: &str = "\
impl Handle {
    #[read(op)]
    async fn get(&self) -> Result<Option<ResolvedOf<T>>, HandleError> {
        Ok(op.get(Kind::<T>::ENTRIES, &()).await?)
    }
}
";
    let rendered = expand_methods(ARGS, RESOLVING)?.to_string();
    assert!(
        rendered.contains("ContextOf < '__ctx , T >"),
        "the resolver context bound must be attached for the returned type: {rendered}"
    );
    Ok(())
}

/// The escape hatch for a resolved type the return type does not spell:
/// `resolve(T)` attaches the same bound, once per distinct type.
#[test]
fn explicit_resolve_attaches_one_bound_per_type() -> Result<(), Error> {
    const RESOLVING: &str = "\
impl Handle {
    #[read(op, resolve(Cart), resolve(Cart))]
    async fn get(&self) -> Result<Option<Cart>, HandleError> {
        Ok(op.get(Kind::ENTRIES, &()).await?)
    }
}
";
    let rendered = expand_methods(ARGS, RESOLVING)?.to_string();
    assert_eq!(
        rendered.matches("ContextOf < '__ctx , Cart >").count(),
        1,
        "a repeated `resolve` attaches one predicate: {rendered}"
    );
    assert!(
        !rendered.contains("ResolvedOf"),
        "the bound is attached without the return type naming it: {rendered}"
    );
    Ok(())
}

const DUPLICATE_ID: &str = "\
struct Kind {
    #[id(0)]
    ALPHA: Cell,
    #[id(0)]
    BETA: Cell,
}
";

const MISSING_AND_OVER_RANGE_ID: &str = "\
struct Kind {
    #[id(200)]
    ALPHA: Cell,
    BETA: Cell,
}
";

const NEGATIVE_ID: &str = "\
struct Kind {
    #[id(-1)]
    ALPHA: Cell,
}
";

const SUFFIXED_ID: &str = "\
struct Kind {
    #[id(7usize)]
    ALPHA: Cell,
}
";

const NON_LITERAL_ID: &str = "\
struct Kind {
    #[id(ENTRIES_ID)]
    ALPHA: Cell,
}
";

const SECOND_ID_ATTRIBUTE: &str = "\
struct Kind {
    #[id(0)]
    #[id(1)]
    ALPHA: Cell,
}
";

const RESERVED_COLLISION: &str = "\
#[reserved_ids(1)]
struct Kind {
    #[id(0)]
    ALPHA: Cell,
    #[id(1)]
    BETA: Cell,
}
";

const REPEATED_RESERVED: &str = "\
#[reserved_ids(4, 4)]
struct Kind {
    #[id(0)]
    ALPHA: Cell,
}
";

const TUPLE_LAYOUT: &str = "struct Kind(Cell);";

const EMPTY_LAYOUT: &str = "\
struct Kind {
}
";

const LIFETIME_PARAMETER: &str = "\
struct Kind<'a> {
    #[id(0)]
    ALPHA: Cell,
}
";

const FIELD_VISIBILITY: &str = "\
struct Kind {
    #[id(0)]
    pub ALPHA: Cell,
}
";

#[test]
fn duplicate_id_rejected_at_the_second_literal() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_layout(DUPLICATE_ID)?,
        &[(
            "durable id 0 is already declared in this layout; every family needs its own id",
            (4, 9, 4, 10),
        )],
    )
}

/// Independent mistakes accumulate into one diagnostic list rather than
/// costing the author a rebuild each.
#[test]
fn every_bad_id_in_one_layout_is_reported() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_layout(MISSING_AND_OVER_RANGE_ID)?,
        &[
            (
                "a durable id is a section discriminant in 0..=127",
                (2, 9, 2, 12),
            ),
            (
                "every cell family needs an explicit durable id, e.g. `#[id(0)]`; ids address \
                 persisted rows and can never be inferred from declaration order",
                (4, 4, 4, 8),
            ),
        ],
    )
}

#[test]
fn negative_id_rejected_at_the_literal() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_layout(NEGATIVE_ID)?,
        &[(
            "a durable id is a section discriminant in 0..=127",
            (2, 9, 2, 11),
        )],
    )
}

#[test]
fn suffixed_id_literal_rejected() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_layout(SUFFIXED_ID)?,
        &[(
            "a durable id is a plain integer literal, e.g. `#[id(0)]`",
            (2, 9, 2, 15),
        )],
    )
}

#[test]
fn non_literal_id_rejected() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_layout(NON_LITERAL_ID)?,
        &[(
            "expected a durable id literal, e.g. `#[id(0)]`",
            (2, 9, 2, 19),
        )],
    )
}

/// A second `#[id(..)]` is rejected rather than re-emitted onto the generated
/// family constant, where it would cascade as an unknown attribute.
#[test]
fn second_id_attribute_rejected() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_layout(SECOND_ID_ATTRIBUTE)?,
        &[("a cell family carries one durable id", (3, 4, 3, 12))],
    )
}

/// The reserved literal names a removed family and must never be touched, so
/// the collision is reported at the active declaration instead.
#[test]
fn reserved_and_active_id_rejected_at_the_active_literal() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_layout(RESERVED_COLLISION)?,
        &[(
            "durable id 1 is reserved and also declared; a reserved id names a removed family and \
             can never be reused",
            (5, 9, 5, 10),
        )],
    )
}

#[test]
fn repeated_reserved_id_rejected_at_the_second_literal() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_layout(REPEATED_RESERVED)?,
        &[(
            "durable id 4 is already reserved in this layout",
            (1, 18, 1, 19),
        )],
    )
}

#[test]
fn tuple_layout_rejected() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_layout(TUPLE_LAYOUT)?,
        &[(
            "a collection layout declares named cell families, e.g. `#[id(0)] ENTRIES: T`",
            (1, 11, 1, 17),
        )],
    )
}

#[test]
fn layout_without_a_family_rejected() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_layout(EMPTY_LAYOUT)?,
        &[(
            "a collection layout declares at least one cell family; a layout with none has no \
             reset domain",
            (1, 7, 1, 11),
        )],
    )
}

#[test]
fn non_type_generic_parameter_rejected() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_layout(LIFETIME_PARAMETER)?,
        &[(
            "a collection layout takes only type parameters; the kind type is zero-sized and \
             carries no lifetime or const state",
            (1, 12, 1, 14),
        )],
    )
}

#[test]
fn field_visibility_rejected() -> Result<(), Error> {
    assert_diagnostics(
        diagnose_layout(FIELD_VISIBILITY)?,
        &[(
            "a cell family carries no visibility of its own; the generated family constant is \
             always `pub(crate)`",
            (3, 4, 3, 7),
        )],
    )
}

#[test]
fn layout_emits_sorted_sections_and_a_descriptor_entry_per_family() -> Result<(), Error> {
    const LAYOUT: &str = "\
struct Kind<T> {
    #[id(3)]
    RIGHT: T,
    #[id(0)]
    LEFT: T,
}
";
    let tokens: TokenStream = parse_str(LAYOUT)?;
    let rendered = layout::expand(tokens)?.to_string();
    assert!(
        rendered
            .contains("Section :: new (0i8) , crate :: state :: cell_key :: Section :: new (3i8)"),
        "SECTIONS is id-sorted, not declaration-ordered: {rendered}"
    );
    assert_eq!(
        rendered.matches("LayoutEntry :: new").count(),
        2,
        "one descriptor entry per active family: {rendered}"
    );
    assert!(
        rendered.contains("const RESERVED : & 'static [i8] = & [] ;"),
        "a layout with no removals reserves nothing: {rendered}"
    );
    Ok(())
}

/// Reserved ids join the reset domain and are emitted in id order, whatever
/// order they were declared in.
#[test]
fn reserved_ids_are_emitted_sorted_and_join_the_reset_domain() -> Result<(), Error> {
    const LAYOUT: &str = "\
#[reserved_ids(5, 2)]
struct Kind<T> {
    #[id(3)]
    RIGHT: T,
    #[id(0)]
    LEFT: T,
}
";
    let tokens: TokenStream = parse_str(LAYOUT)?;
    let rendered = layout::expand(tokens)?.to_string();
    assert!(
        rendered.contains(
            "Section :: new (0i8) , crate :: state :: cell_key :: Section :: new (2i8) , crate :: \
             state :: cell_key :: Section :: new (3i8) , crate :: state :: cell_key :: Section :: \
             new (5i8)"
        ),
        "the reset domain is every active and reserved id, sorted: {rendered}"
    );
    assert!(
        rendered.contains("const RESERVED : & 'static [i8] = & [2i8 , 5i8] ;"),
        "RESERVED is id-sorted: {rendered}"
    );
    Ok(())
}

#[test]
fn impl_block_without_a_marked_method_rejected() -> Result<(), Error> {
    const UNMARKED: &str = "\
impl Handle {
    fn plain(&self) -> u32 {
        0
    }
}
";
    assert_diagnostics(
        diagnose_methods(ARGS, UNMARKED)?,
        &[(
            "`#[collection_methods]` found no `#[read(op)]` or `#[write(op)]` method in this impl \
             block",
            (1, 0, 1, 5),
        )],
    )
}

/// Keeps the entry point wired to the tested rewriter: a malformed attribute
/// argument list must still emit the block plus its rejection, with the
/// operation markers stripped so the rejection is not buried under "cannot
/// find attribute".
#[test]
fn malformed_arguments_emit_the_stripped_block_and_the_rejection() -> Result<(), Error> {
    let args: TokenStream = parse_str("collection = cells")?;
    let item: TokenStream = parse_str(NO_SELF)?;
    let rendered = methods::expand(args, item).to_string();
    assert!(
        rendered.contains("compile_error !"),
        "a malformed argument list must be reported: {rendered}"
    );
    assert!(
        rendered.contains("async fn good"),
        "the authored block must still be emitted: {rendered}"
    );
    assert!(
        !rendered.contains("read (op)"),
        "the operation markers must not survive onto the emitted block: {rendered}"
    );
    Ok(())
}
