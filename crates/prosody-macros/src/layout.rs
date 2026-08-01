//! `collection_layout!` — one collection kind's durable section layout.
//!
//! The declaration is a struct-shaped syntax whose fields are cell families
//! rather than runtime state: the emitted kind type is zero-sized and the
//! fields become associated family tokens. Ids are the durable part, so every
//! validation here is spanned at the literal that owns the mistake.

use crate::combine;
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{
    Attribute, Error, Expr, ExprLit, ExprUnary, Field, Fields, GenericParam, Generics, Ident,
    ItemStruct, Lit, LitInt, Token, Type, UnOp, Visibility, WhereClause, WherePredicate,
};

/// The highest durable id a section can carry: sections are persisted as
/// `i8` discriminants.
const MAX_ID: i64 = i8::MAX as i64;

/// Expands one `collection_layout!` declaration.
pub(crate) fn expand(input: TokenStream) -> Result<TokenStream, Error> {
    Layout::parse(syn::parse2(input)?).map(|layout| layout.emit())
}

/// One declared cell family: its durable id, its token name, and the cell type
/// it stores.
struct Family {
    attrs: Vec<Attribute>,
    id: i64,
    id_span: Span,
    name: Ident,
    ty: Type,
}

/// A parsed, fully validated layout declaration.
struct Layout {
    attrs: Vec<Attribute>,
    vis: Visibility,
    ident: Ident,
    generics: Generics,
    families: Vec<Family>,
    reserved: Vec<(i64, Span)>,
}

impl Layout {
    /// Validates one declaration, combining every independent rejection so a
    /// malformed layout reports all of its mistakes at once.
    fn parse(item: ItemStruct) -> Result<Self, Error> {
        let ItemStruct {
            attrs,
            vis,
            ident,
            generics,
            fields,
            ..
        } = item;
        let mut errors: Option<Error> = None;

        for param in &generics.params {
            if !matches!(param, GenericParam::Type(_)) {
                combine(
                    &mut errors,
                    Error::new(
                        param.span(),
                        "a collection layout takes only type parameters; the kind type is \
                         zero-sized and carries no lifetime or const state",
                    ),
                );
            }
        }

        let (attrs, reserved) = split_reserved(attrs, &mut errors);

        let named = match fields {
            Fields::Named(named) => named.named,
            other => {
                let span = match &other {
                    Fields::Unnamed(unnamed) => unnamed.span(),
                    _ => ident.span(),
                };
                combine(
                    &mut errors,
                    Error::new(
                        span,
                        "a collection layout declares named cell families, e.g. `#[id(0)] \
                         ENTRIES: T`",
                    ),
                );
                Punctuated::new()
            }
        };

        let families = parse_families(named, &mut errors);

        let mut seen: Vec<i64> = Vec::with_capacity(families.len());
        for family in &families {
            if seen.contains(&family.id) {
                combine(
                    &mut errors,
                    Error::new(
                        family.id_span,
                        format!(
                            "durable id {} is already declared in this layout; every family needs \
                             its own id",
                            family.id
                        ),
                    ),
                );
            }
            seen.push(family.id);
        }
        for (id, span) in &reserved {
            if seen.contains(id) {
                combine(
                    &mut errors,
                    Error::new(
                        *span,
                        format!(
                            "durable id {id} is reserved and also declared; a reserved id names a \
                             removed family and can never be reused"
                        ),
                    ),
                );
            }
        }

        if let Some(error) = errors {
            return Err(error);
        }
        Ok(Self {
            attrs,
            vis,
            ident,
            generics,
            families,
            reserved,
        })
    }

    /// The canonical reset domain: every active id plus every reserved
    /// historical one, sorted.
    fn sections(&self) -> Vec<i64> {
        let mut sections: Vec<i64> = self
            .families
            .iter()
            .map(|family| family.id)
            .chain(self.reserved.iter().map(|&(id, _)| id))
            .collect();
        sections.sort_unstable();
        sections
    }

    fn emit(&self) -> TokenStream {
        let Self {
            attrs,
            vis,
            ident,
            generics,
            families,
            reserved,
        } = self;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
        let params = generics.type_params().map(|param| &param.ident);

        let tokens = families.iter().map(|family| {
            let Family {
                attrs, name, ty, ..
            } = family;
            let id = id_literal(family.id, family.id_span);
            quote! {
                #(#attrs)*
                pub(crate) const #name: crate::state::collection::CellFamily<Self, #ty> =
                    crate::state::collection::CellFamily::declare(#id);
            }
        });

        let mut ordered: Vec<&Family> = families.iter().collect();
        ordered.sort_unstable_by_key(|family| family.id);
        let entries = ordered.iter().map(|family| {
            let id = id_literal(family.id, family.id_span);
            let ty = &family.ty;
            quote! {
                crate::state::collection::LayoutEntry::new(
                    #id,
                    <<#ty as crate::state::descriptor::CellType>::Key
                        as crate::codec::Codec>::FORMAT_ID,
                    <<#ty as crate::state::descriptor::CellType>::Codec
                        as crate::codec::Codec>::FORMAT_ID,
                )
            }
        });

        let sections = self
            .sections()
            .into_iter()
            .map(|id| id_literal(id, ident.span()));
        let reserved = reserved.iter().map(|&(id, span)| id_literal(id, span));
        let layout_where = layout_where_clause(generics, families);

        quote! {
            #(#attrs)*
            ///
            /// # Durable ids
            ///
            /// Each family's `#[id(n)]` addresses persisted rows. Never change
            /// or reuse an existing id: reordering declarations is harmless,
            /// but a changed id silently re-points a family at another
            /// family's stored cells. Reserve the number with
            /// `#[reserved_ids(n)]` when a family is removed, so a whole-layout
            /// reset keeps erasing its legacy rows.
            #vis struct #ident #generics (
                ::core::marker::PhantomData<fn() -> ( #(#params,)* )>
            ) #where_clause;

            impl #impl_generics #ident #ty_generics #where_clause {
                #(#tokens)*
            }

            impl #impl_generics crate::state::collection::CollectionLayout
                for #ident #ty_generics
            #layout_where
            {
                const SECTIONS: &'static [crate::state::cell_key::Section] =
                    &[ #(crate::state::cell_key::Section::new(#sections)),* ];

                const DESCRIPTOR: &'static [crate::state::collection::LayoutEntry] =
                    &[ #(#entries),* ];

                const RESERVED: &'static [i8] = &[ #(#reserved),* ];
            }

            impl #impl_generics crate::state::collection::sealed_spec::SealedSpec
                for #ident #ty_generics #where_clause {}
        }
    }
}

/// Reads one declaration's fields into families, reporting a field that
/// carries no durable id at its own name.
fn parse_families(named: Punctuated<Field, Token![,]>, errors: &mut Option<Error>) -> Vec<Family> {
    let mut families = Vec::with_capacity(named.len());
    for field in named {
        let Some(name) = field.ident else { continue };
        let (id_attr, attrs) = split_id(field.attrs);
        let Some(id_attr) = id_attr else {
            combine(
                errors,
                Error::new(
                    name.span(),
                    "every cell family needs an explicit durable id, e.g. `#[id(0)]`; ids address \
                     persisted rows and can never be inferred from declaration order",
                ),
            );
            continue;
        };
        match parse_id(&id_attr) {
            Ok((id, id_span)) => families.push(Family {
                attrs,
                id,
                id_span,
                name,
                ty: field.ty,
            }),
            Err(error) => combine(errors, error),
        }
    }
    families
}

/// Splits `#[reserved_ids(…)]` out of the declaration's outer attributes,
/// returning the remaining attributes and the validated reserved ids.
fn split_reserved(
    attrs: Vec<Attribute>,
    errors: &mut Option<Error>,
) -> (Vec<Attribute>, Vec<(i64, Span)>) {
    let mut kept = Vec::with_capacity(attrs.len());
    let mut reserved: Vec<(i64, Span)> = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("reserved_ids") {
            kept.push(attr);
            continue;
        }
        let parsed = attr.parse_args_with(Punctuated::<Expr, Token![,]>::parse_terminated);
        match parsed {
            Ok(exprs) => {
                for expr in &exprs {
                    match id_value(expr) {
                        Ok(id) => reserved.push((id, expr.span())),
                        Err(error) => combine(errors, error),
                    }
                }
            }
            Err(error) => combine(errors, error),
        }
    }
    (kept, reserved)
}

/// Splits the `#[id(n)]` attribute out of one field's attributes.
fn split_id(attrs: Vec<Attribute>) -> (Option<Attribute>, Vec<Attribute>) {
    let mut id = None;
    let mut kept = Vec::with_capacity(attrs.len());
    for attr in attrs {
        if id.is_none() && attr.path().is_ident("id") {
            id = Some(attr);
        } else {
            kept.push(attr);
        }
    }
    (id, kept)
}

/// Reads one `#[id(n)]` attribute's value and the span of its literal.
fn parse_id(attr: &Attribute) -> Result<(i64, Span), Error> {
    let expr: Expr = attr.parse_args()?;
    let span = expr.span();
    id_value(&expr).map(|id| (id, span))
}

/// Validates one id expression: a plain integer literal in `0..=127`. A
/// negated literal is rejected at the whole expression, which is where the
/// author wrote the sign.
fn id_value(expr: &Expr) -> Result<i64, Error> {
    let literal = match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Int(literal),
            ..
        }) => literal,
        Expr::Unary(ExprUnary {
            op: UnOp::Neg(_), ..
        }) => {
            return Err(Error::new(
                expr.span(),
                format!("a durable id is a section discriminant in 0..={MAX_ID}"),
            ));
        }
        other => {
            return Err(Error::new(
                other.span(),
                "expected a durable id literal, e.g. `#[id(0)]`",
            ));
        }
    };
    let value: i64 = literal.base10_parse()?;
    if value > MAX_ID {
        return Err(Error::new(
            literal.span(),
            format!("a durable id is a section discriminant in 0..={MAX_ID}"),
        ));
    }
    Ok(value)
}

/// The `where` clause of the generated `CollectionLayout` implementation: the
/// declaration's own predicates plus the cell-type bound each family's
/// descriptor entry reads its format tokens through.
fn layout_where_clause(generics: &Generics, families: &[Family]) -> WhereClause {
    let mut clause = generics.where_clause.clone().unwrap_or(WhereClause {
        where_token: Token![where](Span::call_site()),
        predicates: Punctuated::new(),
    });
    for family in families {
        let ty = &family.ty;
        let predicate =
            syn::parse2::<WherePredicate>(quote!(#ty: crate::state::descriptor::CellType));
        if let Ok(predicate) = predicate {
            clause.predicates.push(predicate);
        }
    }
    clause
}

/// An `i8`-suffixed literal for a validated id.
fn id_literal(id: i64, span: Span) -> LitInt {
    LitInt::new(&format!("{id}i8"), span)
}
