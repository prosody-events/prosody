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

mod families;
mod impls;

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
