//! Diagnostics and expansions of the method macro on impl blocks.

use super::*;

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
    fn items(&self) -> impl Stream<Item = u32> + use<'_> {
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
