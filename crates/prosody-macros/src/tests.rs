//! Diagnostic pins for both macros.
//!
//! Every fixture is parsed from a source string (`syn::parse_str`), never
//! `parse_quote!`: minted tokens all carry the call site, so only a real parse
//! gives the `syn::Error` a line and column to assert. `proc-macro2`'s
//! `span-locations` feature (a dev-dependency here) is what makes those
//! locations readable.
//!
//! Each test pins the message *and* the exact token the diagnostic covers,
//! because a correct message on the wrong token is still a bad diagnostic.

use crate::methods::{Args, rewrite};
use crate::{layout, methods};
use proc_macro2::{Span, TokenStream};
use quote::ToTokens;
use syn::{Error, ItemImpl, parse_str};

/// The `self`-ban message, shared by the two fixtures that trip it.
const SELF_MESSAGE: &str = "a marked collection method body may not reference `self`; use `op`, a \
                            method argument, or a free helper taking `&mut impl CollectionRead`";

/// Rewrites one `impl` fixture and returns the rejection it produced, if any.
fn diagnose_methods(args: &str, source: &str) -> Result<Option<Error>, Error> {
    let args: Args = parse_str(args)?;
    let mut item: ItemImpl = parse_str(source)?;
    Ok(rewrite(&mut item, &args))
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

/// Asserts a rejection's message and the exact `(line, column)` span it
/// covers. Columns are zero-based, lines one-based, as `proc-macro2` reports
/// them.
#[track_caller]
fn assert_diagnostic(
    error: Option<Error>,
    message: &str,
    span: (usize, usize, usize, usize),
) -> Result<(), Error> {
    let Some(error) = error else {
        return Err(Error::new(
            Span::call_site(),
            "expected a rejection, got an accepted expansion",
        ));
    };
    assert_eq!(error.to_string(), message, "diagnostic message");
    let located = error.span();
    let (start, end) = (located.start(), located.end());
    assert_eq!(
        (start.line, start.column, end.line, end.column),
        span,
        "diagnostic must cover the responsible token"
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

const NO_SELF: &str = "\
impl Handle {
    #[read(op)]
    async fn good(&self, n: u32) -> Result<u32, HandleError> {
        let window = bounds(op).await?;
        Ok(window + n)
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

const NON_ASYNC_WRITE: &str = "\
impl Handle {
    #[write(op)]
    fn bad(&self) -> u32 {
        0
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

const OP_BINDING: &str = "\
impl Handle {
    #[read(op)]
    async fn bad(&self) -> u32 {
        let op = 1;
        op
    }
}
";

const STREAM: &str = "\
impl Handle {
    #[read(op)]
    fn items(&self) -> impl Stream<Item = u32> + '_ {
        let window = bounds(op).await?;
        op.coordinates(window)
    }
}
";

#[test]
fn self_in_marked_body_reported_at_self_token() -> Result<(), Error> {
    assert_diagnostic(
        diagnose_methods("field = cells, session = S", SELF_IN_TAIL)?,
        SELF_MESSAGE,
        (4, 8, 4, 12),
    )?;
    Ok(())
}

#[test]
fn nested_self_reported_at_self_token() -> Result<(), Error> {
    assert_diagnostic(
        diagnose_methods("field = cells, session = S", SELF_NESTED)?,
        SELF_MESSAGE,
        (5, 36, 5, 40),
    )?;
    Ok(())
}

#[test]
fn op_only_and_unmarked_methods_accepted() -> Result<(), Error> {
    let expansion = expand_methods("field = cells, session = S", NO_SELF)?;
    let rendered = expansion.to_string();
    assert!(
        rendered.contains("cells . read"),
        "the marked method must run in a read scope: {rendered}"
    );
    assert!(
        rendered.contains("self . cells . len"),
        "an unmarked method must be copied through untouched: {rendered}"
    );
    Ok(())
}

#[test]
fn mut_self_receiver_rejected() -> Result<(), Error> {
    assert_diagnostic(
        diagnose_methods("field = cells, session = S", MUT_SELF)?,
        "a marked collection method takes `&self`; admission is acquired per invocation, so the \
         handle is never mutated",
        (3, 22, 3, 26),
    )?;
    Ok(())
}

#[test]
fn non_async_write_rejected() -> Result<(), Error> {
    assert_diagnostic(
        diagnose_methods("field = cells, session = S", NON_ASYNC_WRITE)?,
        "a `#[write(op)]` method is `async`: it acquires write admission once, and no write \
         method streams",
        (2, 6, 2, 11),
    )?;
    Ok(())
}

#[test]
fn op_argument_collision_rejected() -> Result<(), Error> {
    assert_diagnostic(
        diagnose_methods("field = cells, session = S", OP_ARGUMENT)?,
        "`op` names the scoped operation inside this body; rename the argument",
        (3, 24, 3, 26),
    )?;
    Ok(())
}

#[test]
fn op_binding_shadow_rejected() -> Result<(), Error> {
    assert_diagnostic(
        diagnose_methods("field = cells, session = S", OP_BINDING)?,
        "`op` names the scoped operation inside this body; rename the binding",
        (4, 12, 4, 14),
    )?;
    Ok(())
}

#[test]
fn missing_session_argument_rejected() -> Result<(), Error> {
    assert_diagnostic(
        diagnose_methods("field = cells", NON_ASYNC_WRITE)?,
        "`#[collection_methods]` needs `session = <ident>` naming the impl's session type \
         parameter; the write and resolver bounds on marked methods are attached to it",
        (1, 0, 1, 5),
    )?;
    Ok(())
}

#[test]
fn stream_method_lowered_to_a_read_plan() -> Result<(), Error> {
    let rendered = expand_methods("field = cells, session = S", STREAM)?.to_string();
    assert!(
        rendered.contains("drive_plan"),
        "a non-async marked method's body becomes its stream plan: {rendered}"
    );
    assert!(
        rendered.contains("op . coordinates (window)"),
        "the authored plan tail must survive verbatim: {rendered}"
    );
    Ok(())
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
    let rendered = expand_methods("field = cells, session = S", RESOLVING)?.to_string();
    assert!(
        rendered.contains("ContextOf < '__ctx , T >"),
        "the resolver context bound must be attached for the returned type: {rendered}"
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

const MISSING_ID: &str = "\
struct Kind {
    ENTRIES: Cell,
}
";

const OVER_RANGE_ID: &str = "\
struct Kind {
    #[id(128)]
    ALPHA: Cell,
}
";

const NEGATIVE_ID: &str = "\
struct Kind {
    #[id(-1)]
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

#[test]
fn duplicate_id_rejected_at_the_second_literal() -> Result<(), Error> {
    assert_diagnostic(
        diagnose_layout(DUPLICATE_ID)?,
        "durable id 0 is already declared in this layout; every family needs its own id",
        (4, 9, 4, 10),
    )?;
    Ok(())
}

#[test]
fn missing_id_rejected_at_the_field_name() -> Result<(), Error> {
    assert_diagnostic(
        diagnose_layout(MISSING_ID)?,
        "every cell family needs an explicit durable id, e.g. `#[id(0)]`; ids address persisted \
         rows and can never be inferred from declaration order",
        (2, 4, 2, 11),
    )?;
    Ok(())
}

#[test]
fn out_of_range_id_rejected_at_the_literal() -> Result<(), Error> {
    assert_diagnostic(
        diagnose_layout(OVER_RANGE_ID)?,
        "a durable id is a section discriminant in 0..=127",
        (2, 9, 2, 12),
    )?;
    Ok(())
}

#[test]
fn negative_id_rejected_at_the_literal() -> Result<(), Error> {
    assert_diagnostic(
        diagnose_layout(NEGATIVE_ID)?,
        "a durable id is a section discriminant in 0..=127",
        (2, 9, 2, 11),
    )?;
    Ok(())
}

#[test]
fn reserved_and_active_id_rejected_at_the_reserved_literal() -> Result<(), Error> {
    assert_diagnostic(
        diagnose_layout(RESERVED_COLLISION)?,
        "durable id 1 is reserved and also declared; a reserved id names a removed family and can \
         never be reused",
        (1, 15, 1, 16),
    )?;
    Ok(())
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
    let sections = rendered
        .split("SECTIONS")
        .nth(1)
        .unwrap_or_default()
        .split("DESCRIPTOR")
        .next()
        .unwrap_or_default();
    assert!(
        sections.find("(0i8)") < sections.find("(3i8)"),
        "SECTIONS is id-sorted, not declaration-ordered: {sections}"
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

/// The attribute is a no-op on an impl block with nothing marked; saying so is
/// cheaper than a reader wondering why the macro is there.
#[test]
fn impl_block_without_a_marked_method_rejected() -> Result<(), Error> {
    const UNMARKED: &str = "\
impl Handle {
    fn plain(&self) -> u32 {
        0
    }
}
";
    assert_diagnostic(
        diagnose_methods("field = cells, session = S", UNMARKED)?,
        "`#[collection_methods]` found no `#[read(op)]` or `#[write(op)]` method in this impl \
         block",
        (1, 0, 1, 5),
    )?;
    Ok(())
}

/// Keeps the entry point wired to the tested rewriter: a malformed attribute
/// argument list must still emit the block plus its rejection.
#[test]
fn malformed_arguments_emit_the_block_and_the_rejection() -> Result<(), Error> {
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
    Ok(())
}
