//! Framework-internal procedural macros for prosody's keyed-state collection
//! authors.
//!
//! Two entry points, both generating `crate::state::collection::…` paths, so
//! they expand correctly only inside `prosody` itself:
//!
//! - [`collection_layout!`] declares a collection kind's durable section
//!   layout: one explicitly numbered field per cell family. It emits the
//!   zero-sized kind type, its family tokens, and the generated layout
//!   descriptor the frozen-layout tests pin.
//! - [`collection_methods`] rewrites the marked methods of one handle `impl`
//!   block so each public invocation runs as exactly one scoped collection
//!   operation.
//!
//! Diagnostics are part of the interface: every rejection is a `syn::Error`
//! spanned at the smallest responsible token and states the correction. The
//! macros never panic, unwrap, or index unchecked on malformed input.

use proc_macro::TokenStream;
use syn::Error;

mod layout;
mod methods;
mod selfban;

#[cfg(test)]
mod tests;

/// Accumulates independent rejections into one diagnostic, so a malformed
/// declaration reports every mistake instead of one per rebuild.
pub(crate) fn combine(slot: &mut Option<Error>, error: Error) {
    match slot {
        Some(accumulated) => accumulated.combine(error),
        None => *slot = Some(error),
    }
}

/// Declares one collection kind's durable layout.
///
/// ```ignore
/// collection_layout! {
///     /// A two-family collection.
///     #[reserved_ids(2)]
///     pub struct QueueKind<T> {
///         #[id(0)]
///         BOUNDS: BoundsCell,
///         #[id(1)]
///         ITEMS: T,
///     }
/// }
/// ```
///
/// Every field needs an explicit protobuf-style `#[id(n)]` in `0..=127`;
/// declaration order is cosmetic and reordering is safe. Removing a field
/// reserves its number through `#[reserved_ids(…)]` so the whole-layout reset
/// keeps erasing its legacy rows. The macro rejects a missing, duplicate,
/// negative, out-of-range, or reserved-and-active id at the exact literal.
///
/// The expansion is the kind type itself (zero-sized), one
/// `CellFamily` associated constant per field, the sealed `CollectionLayout`
/// implementation carrying the canonical section set and layout descriptor,
/// and the marker that seals `CollectionSpec`.
#[proc_macro]
pub fn collection_layout(input: TokenStream) -> TokenStream {
    layout::expand(input.into())
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Rewrites one collection handle `impl` block so each marked method runs as
/// exactly one scoped operation.
///
/// ```ignore
/// #[collection_methods(field = cells, session = S)]
/// impl<S, T> QueueHandle<S, T>
/// where
///     S: StateSession,
///     T: CellType<Key = QueueIndex>,
/// {
///     #[read(op)]
///     pub async fn front(&self) -> Result<Option<ResolvedOf<T>>, QueueError> {
///         Ok(op.get(QueueKind::<T>::ITEMS, &0).await?)
///     }
/// }
/// ```
///
/// `field` names the handle field holding the bound collection; `session`
/// names the impl's session type parameter, which the macro needs to attach
/// the write and resolver bounds. Naming it is deliberate rather than inferred
/// from the field's type tokens: an alias, a same-named foreign trait, or a
/// multi-parameter impl all defeat token inference.
///
/// - `#[read(op)]` on an `async fn` wraps the body in one read scope;
/// - `#[write(op)]` wraps it in one write scope and adds a method-local
///   writable-session bound;
/// - `#[read(op)]` on a non-`async fn` returning a stream treats the body as
///   that stream's async plan;
/// - unmarked methods are copied through untouched.
///
/// The written method stays the public method: visibility, name, receiver,
/// arguments, return type, generics, `where` clauses, rustdoc, and tracing
/// attributes are preserved verbatim with their source spans. Resolver context
/// bounds are added for every `ResolvedOf<T>` in the written return type, or
/// for the type named by an explicit `#[read(op, resolve(T))]`.
///
/// A marked body may not name `self`: recursive acquisition of the admission
/// the body already holds is made unexpressible rather than checked at
/// runtime. Stateful helpers take `&mut impl CollectionRead` or
/// `&mut impl CollectionWrite` instead.
#[proc_macro_attribute]
pub fn collection_methods(args: TokenStream, item: TokenStream) -> TokenStream {
    methods::expand(args.into(), item.into()).into()
}
