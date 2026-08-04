//! Body scanning for marked collection methods.
//!
//! Two rejections need the author's own tokens: a `self` reference (recursive
//! admission would deadlock the gate the body already holds) and a binding
//! that shadows the operation identifier (the shadow would silently take the
//! commands away from the body that needs them). Both are found by one visit
//! over the method block, and both report the offending identifier rather than
//! its enclosing expression.
//!
//! A macro invocation carries unparsed tokens, so its arguments are scanned for
//! a bare `self` ident instead of visited — `tokio::join!(self.get(), …)` is
//! exactly the concurrent re-entry the ban exists for. The residual: a macro
//! that *synthesizes* `self` is invisible to the scan, and equally unable to
//! reach a marked method, since nothing hands it a receiver.

use proc_macro2::{Span, TokenStream, TokenTree};
use syn::visit::{Visit, visit_expr_path, visit_macro, visit_pat_ident};
use syn::{Block, ExprPath, Ident, Item, Macro, Pat, PatIdent};

/// What one marked body's scan found.
pub(crate) struct BodyScan {
    /// Spans of every `self` expression in the body.
    pub(crate) self_refs: Vec<Span>,
    /// Spans of every binding that would shadow the operation identifier.
    pub(crate) op_shadows: Vec<Span>,
}

/// The visitor behind both scans.
struct Finder<'a> {
    op: &'a Ident,
    scan: BodyScan,
}

impl<'a> Finder<'a> {
    fn new(op: &'a Ident) -> Self {
        Self {
            op,
            scan: BodyScan {
                self_refs: Vec::new(),
                op_shadows: Vec::new(),
            },
        }
    }
}

impl<'ast> Visit<'ast> for Finder<'_> {
    fn visit_expr_path(&mut self, node: &'ast ExprPath) {
        // Only a lone `self` is the receiver: `self::helper(op)` is a module
        // path, which the diagnostic itself recommends.
        if let Some(ident) = node.path.get_ident()
            && ident == "self"
        {
            self.scan.self_refs.push(ident.span());
        }
        visit_expr_path(self, node);
    }

    fn visit_item(&mut self, _: &'ast Item) {
        // An item declared inside the body captures neither the receiver nor
        // the operation, so nothing within one can trip either rejection.
    }

    fn visit_macro(&mut self, node: &'ast Macro) {
        push_self_tokens(node.tokens.clone(), &mut self.scan.self_refs);
        visit_macro(self, node);
    }

    fn visit_pat_ident(&mut self, node: &'ast PatIdent) {
        if node.ident == *self.op {
            self.scan.op_shadows.push(node.ident.span());
        }
        visit_pat_ident(self, node);
    }
}

/// Scans one marked method body against the operation identifier `op`.
pub(crate) fn scan(body: &Block, op: &Ident) -> BodyScan {
    let mut finder = Finder::new(op);
    finder.visit_block(body);
    finder.scan
}

/// Spans of every binding in one argument pattern that would shadow `op`,
/// including the bindings of a destructuring pattern.
pub(crate) fn pattern_shadows(pattern: &Pat, op: &Ident) -> Vec<Span> {
    let mut finder = Finder::new(op);
    finder.visit_pat(pattern);
    finder.scan.op_shadows
}

/// Records the span of every bare `self` ident in one macro's argument tokens.
fn push_self_tokens(tokens: TokenStream, found: &mut Vec<Span>) {
    for tree in tokens {
        match tree {
            TokenTree::Ident(ident) if ident == "self" => found.push(ident.span()),
            TokenTree::Group(group) => push_self_tokens(group.stream(), found),
            _ => {}
        }
    }
}
