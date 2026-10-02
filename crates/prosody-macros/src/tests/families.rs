//! Diagnostics and expansions of the layout macro.

use super::*;

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
