//! `#[collection_methods]` — one marked method, one scoped operation.
//!
//! The attribute rewrites only the bodies of marked methods: everything the
//! author wrote about the *interface* is re-emitted token-for-token, so a
//! mistake inside a body is reported at the author's own tokens rather than at
//! the attribute. Every generated token is spanned at the authored block for
//! the same reason.

use crate::combine;
use crate::selfban::scan;
use proc_macro2::{Span, TokenStream};
use quote::{ToTokens, quote, quote_spanned};
use std::mem::take;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::visit::{Visit, visit_type_path};
use syn::{
    Attribute, Error, FnArg, GenericArgument, Ident, ImplItem, ImplItemFn, ItemImpl, Pat,
    PathArguments, ReceiverKind, ReturnType, Token, Type, TypePath, WherePredicate, parenthesized,
};

/// Expands the attribute over one handle `impl` block. Malformed input still
/// emits the block, so the author sees the rejection rather than a cascade of
/// "method not found" errors.
pub(crate) fn expand(args: TokenStream, item: TokenStream) -> TokenStream {
    let mut item: ItemImpl = match syn::parse2(item) {
        Ok(item) => item,
        Err(error) => return error.into_compile_error(),
    };
    let mut errors = match syn::parse2::<Args>(args) {
        Ok(args) => rewrite(&mut item, &args),
        Err(error) => Some(error),
    };
    let mut out = item.into_token_stream();
    if let Some(error) = errors.take() {
        out.extend(error.into_compile_error());
    }
    out
}

/// The attribute's arguments: `field = <ident>, session = <ident>`.
pub(crate) struct Args {
    /// The handle field holding the bound collection.
    field: Ident,
    /// The impl's session type parameter, or `None` when unnamed.
    session: Option<Ident>,
    /// The `field` argument's span — where a missing `session` is reported.
    span: Span,
}

impl Parse for Args {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut field: Option<Ident> = None;
        let mut session: Option<Ident> = None;
        let mut span = input.span();
        let pairs = Punctuated::<Pair, Token![,]>::parse_terminated(input)?;
        for pair in pairs {
            if pair.name == "field" {
                span = pair.name.span();
                field = Some(pair.value);
            } else if pair.name == "session" {
                session = Some(pair.value);
            } else {
                return Err(Error::new(
                    pair.name.span(),
                    "expected `field = <ident>` or `session = <ident>`",
                ));
            }
        }
        let Some(field) = field else {
            return Err(Error::new(
                span,
                "`#[collection_methods]` needs `field = <ident>` naming the handle field that \
                 holds the bound collection",
            ));
        };
        Ok(Self {
            field,
            session,
            span,
        })
    }
}

/// One `name = value` attribute argument.
struct Pair {
    name: Ident,
    value: Ident,
}

impl Parse for Pair {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let name: Ident = input.parse()?;
        input.parse::<Token![=]>()?;
        let value: Ident = input.parse()?;
        Ok(Self { name, value })
    }
}

/// Which scope a marked method runs in.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Scope {
    Read,
    Write,
}

/// A parsed operation marker: the binding the body sees plus any explicit
/// resolver types.
struct Marker {
    scope: Scope,
    op: Ident,
    resolve: Vec<Type>,
    span: Span,
}

/// The marker's arguments: `op` plus optional `resolve(T)` entries.
struct MarkerArgs {
    op: Ident,
    resolve: Vec<Type>,
}

impl Parse for MarkerArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let op: Ident = input.parse()?;
        let mut resolve = Vec::new();
        while input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            let key: Ident = input.parse()?;
            if key != "resolve" {
                return Err(Error::new(key.span(), "expected `resolve(<type>)`"));
            }
            let inner;
            parenthesized!(inner in input);
            resolve.push(inner.parse()?);
        }
        Ok(Self { op, resolve })
    }
}

/// Rewrites every marked method of `item` in place, returning the combined
/// diagnostic. Split out of the entry point so the diagnostic tests can drive
/// it from `syn::parse_str` fixtures, whose tokens carry real source
/// locations.
pub(crate) fn rewrite(item: &mut ItemImpl, args: &Args) -> Option<Error> {
    let mut errors: Option<Error> = None;
    let mut found_any = false;
    for entry in &mut item.items {
        let ImplItem::Fn(function) = entry else {
            continue;
        };
        match take_marker(function) {
            Ok(None) => {}
            Ok(Some(marker)) => {
                found_any = true;
                let Some(session) = args.session.clone() else {
                    combine(
                        &mut errors,
                        Error::new(
                            args.span,
                            "`#[collection_methods]` needs `session = <ident>` naming the impl's \
                             session type parameter; the write and resolver bounds on marked \
                             methods are attached to it",
                        ),
                    );
                    continue;
                };
                if let Err(error) = lower(function, &marker, &args.field, &session) {
                    combine(&mut errors, error);
                }
            }
            Err(error) => combine(&mut errors, error),
        }
    }
    if !found_any && errors.is_none() {
        // An impl block with nothing to rewrite is a silent no-op that reads
        // as if the attribute did something. Say so at the attribute.
        combine(
            &mut errors,
            Error::new(
                args.span,
                "`#[collection_methods]` found no `#[read(op)]` or `#[write(op)]` method in this \
                 impl block",
            ),
        );
    }
    errors
}

/// Removes and parses the operation marker from one method's attributes.
fn take_marker(function: &mut ImplItemFn) -> Result<Option<Marker>, Error> {
    let mut found: Option<Marker> = None;
    let mut errors: Option<Error> = None;
    let mut kept: Vec<Attribute> = Vec::with_capacity(function.attrs.len());

    for attr in take(&mut function.attrs) {
        let scope = if attr.path().is_ident("read") {
            Scope::Read
        } else if attr.path().is_ident("write") {
            Scope::Write
        } else {
            kept.push(attr);
            continue;
        };
        let span = attr.path().span();
        match attr.parse_args::<MarkerArgs>() {
            Ok(MarkerArgs { op, resolve }) => {
                if let Some(previous) = &found {
                    combine(
                        &mut errors,
                        Error::new(
                            span,
                            format!(
                                "a method runs in exactly one scope; `{}` is already bound",
                                previous.op
                            ),
                        ),
                    );
                    continue;
                }
                found = Some(Marker {
                    scope,
                    op,
                    resolve,
                    span,
                });
            }
            Err(error) => combine(&mut errors, error),
        }
    }

    function.attrs = kept;
    match errors {
        Some(error) => Err(error),
        None => Ok(found),
    }
}

/// Rewrites one marked method's body into its scope, and attaches the bounds
/// the scope and the method's resolvers require.
fn lower(
    function: &mut ImplItemFn,
    marker: &Marker,
    field: &Ident,
    session: &Ident,
) -> Result<(), Error> {
    if let Some(error) = validate(function, marker) {
        return Err(error);
    }
    let is_async = function.sig.asyncness.is_some();

    let op = &marker.op;
    let body = &function.block;
    let at = body.span();
    let call = match marker.scope {
        Scope::Read => quote_spanned!(at => self.#field.read(async move |#op| #body)),
        Scope::Write => quote_spanned!(at => self.#field.write(async move |#op| #body)),
    };
    function.block = if is_async {
        syn::parse2(quote_spanned!(at => { #call.await }))?
    } else {
        // A non-async marked method's written return type is its stream; the
        // authored body is that stream's async plan, and the private driver
        // turns the plan into the stream. Lowering here is what lets the
        // author write `.await` in an ordinary-looking stream body.
        syn::parse2(quote_spanned!(at => { crate::state::collection::drive_plan(#call) }))?
    };

    // Bounds are the macro's own tokens, not the author's: emitting them at
    // the attribute (rather than at the authored block) keeps a bound-level
    // mistake attributed to the macro, and keeps the generated qualified paths
    // out of the author's lint scope.
    let mut added: Vec<WherePredicate> = Vec::new();
    if marker.scope == Scope::Write {
        added.push(syn::parse2(quote!(
            #session: crate::state::collection::WritableStateSession
        ))?);
    }
    for resolved in resolver_types(marker, &function.sig.output) {
        added.push(syn::parse2(quote!(
            for<'__ctx> crate::state::descriptor::ContextOf<'__ctx, #resolved>:
                crate::state::descriptor::FromSession<'__ctx, #session>
        ))?);
    }
    if !added.is_empty() {
        let predicates = &mut function.sig.generics.make_where_clause().predicates;
        predicates.extend(added);
    }
    Ok(())
}

/// Rejects everything about one marked method that must be fixed before its
/// body can be lowered: the receiver shape, an operation identifier the
/// signature or body would shadow, a streaming write, and any `self`
/// reference.
fn validate(function: &ImplItemFn, marker: &Marker) -> Option<Error> {
    let mut errors: Option<Error> = None;
    let is_async = function.sig.asyncness.is_some();

    match function.sig.inputs.first() {
        Some(FnArg::Receiver(receiver)) => match &receiver.kind {
            ReceiverKind::Reference(_, _, None) => {}
            ReceiverKind::Reference(_, _, Some(_)) => combine(
                &mut errors,
                Error::new(
                    receiver.self_token.span(),
                    "a marked collection method takes `&self`; admission is acquired per \
                     invocation, so the handle is never mutated",
                ),
            ),
            _ => combine(
                &mut errors,
                Error::new(
                    receiver.self_token.span(),
                    "a marked collection method takes `&self`; admission is acquired per \
                     invocation, so the handle is never consumed",
                ),
            ),
        },
        Some(argument) => combine(
            &mut errors,
            Error::new(argument.span(), "a marked collection method takes `&self`"),
        ),
        None => combine(
            &mut errors,
            Error::new(
                function.sig.ident.span(),
                "a marked collection method takes `&self`",
            ),
        ),
    }

    for argument in &function.sig.inputs {
        let FnArg::Typed(typed) = argument else {
            continue;
        };
        let Pat::Ident(pattern) = typed.pat.as_ref() else {
            continue;
        };
        if pattern.ident == marker.op {
            combine(
                &mut errors,
                Error::new(
                    pattern.ident.span(),
                    format!(
                        "`{}` names the scoped operation inside this body; rename the argument",
                        marker.op
                    ),
                ),
            );
        }
    }

    if marker.scope == Scope::Write && !is_async {
        combine(
            &mut errors,
            Error::new(
                marker.span,
                "a `#[write(op)]` method is `async`: it acquires write admission once, and no \
                 write method streams",
            ),
        );
    }

    let body_scan = scan(&function.block, &marker.op);
    for span in body_scan.self_refs {
        combine(
            &mut errors,
            Error::new(
                span,
                format!(
                    "a marked collection method body may not reference `self`; use `{}`, a method \
                     argument, or a free helper taking `&mut impl CollectionRead`",
                    marker.op
                ),
            ),
        );
    }
    for span in body_scan.op_shadows {
        combine(
            &mut errors,
            Error::new(
                span,
                format!(
                    "`{}` names the scoped operation inside this body; rename the binding",
                    marker.op
                ),
            ),
        );
    }

    errors
}

/// The types this method resolves: the marker's explicit `resolve(T)`
/// overrides, or every `ResolvedOf<T>` appearing in the written return type.
fn resolver_types(marker: &Marker, output: &ReturnType) -> Vec<Type> {
    if !marker.resolve.is_empty() {
        return marker.resolve.clone();
    }
    let ReturnType::Type(_, ty) = output else {
        return Vec::new();
    };
    let mut finder = ResolvedFinder { found: Vec::new() };
    finder.visit_type(ty);
    finder.found
}

struct ResolvedFinder {
    found: Vec<Type>,
}

impl<'ast> Visit<'ast> for ResolvedFinder {
    fn visit_type_path(&mut self, node: &'ast TypePath) {
        if let Some(last) = node.path.segments.last()
            && last.ident == "ResolvedOf"
            && let PathArguments::AngleBracketed(arguments) = &last.arguments
            && let Some(GenericArgument::Type(resolved)) = arguments.args.first()
        {
            let rendered = resolved.to_token_stream().to_string();
            if !self
                .found
                .iter()
                .any(|seen| seen.to_token_stream().to_string() == rendered)
            {
                self.found.push(resolved.clone());
            }
        }
        visit_type_path(self, node);
    }
}
