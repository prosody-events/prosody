//! Body scanning for marked collection methods.
//!
//! Two rejections need the author's own tokens: a `self` reference (recursive
//! admission would deadlock the gate the body already holds) and a binding
//! that shadows the operation identifier (the shadow would silently take the
//! commands away from the body that needs them). Both are found by one visit
//! over the method block, and both report the offending identifier rather than
//! its enclosing expression.

use proc_macro2::Span;
use syn::visit::{Visit, visit_expr_path, visit_pat_ident};
use syn::{Block, ExprPath, Ident, PatIdent};

/// What one marked body's scan found, in source order.
pub(crate) struct BodyScan {
    /// Spans of every `self` expression in the body.
    pub(crate) self_refs: Vec<Span>,
    /// Spans of every binding that would shadow the operation identifier.
    pub(crate) op_shadows: Vec<Span>,
}

/// Scans one marked method body against the operation identifier `op`.
pub(crate) fn scan(body: &Block, op: &Ident) -> BodyScan {
    let mut finder = Finder {
        op,
        scan: BodyScan {
            self_refs: Vec::new(),
            op_shadows: Vec::new(),
        },
    };
    finder.visit_block(body);
    finder.scan
}

struct Finder<'a> {
    op: &'a Ident,
    scan: BodyScan,
}

impl<'ast> Visit<'ast> for Finder<'_> {
    fn visit_expr_path(&mut self, node: &'ast ExprPath) {
        if let Some(first) = node.path.segments.first()
            && first.ident == "self"
        {
            self.scan.self_refs.push(first.ident.span());
        }
        visit_expr_path(self, node);
    }

    fn visit_pat_ident(&mut self, node: &'ast PatIdent) {
        if node.ident == *self.op {
            self.scan.op_shadows.push(node.ident.span());
        }
        visit_pat_ident(self, node);
    }
}
