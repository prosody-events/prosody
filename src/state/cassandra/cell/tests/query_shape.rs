/// Every presence query selects write times instead of payload columns.
#[test]
fn presence_queries_exclude_payloads() {
    let source = include_str!("../queries.rs");
    let projections = source
        .lines()
        .filter(|line| line.contains("SELECT") && line.contains("WRITETIME(data)"));
    let projections: Vec<_> = projections.collect();
    assert_eq!(projections.len(), 7, "all seven presence queries exist");
    assert!(
        projections.iter().all(|line| {
            line.contains("WRITETIME(prev_data)")
                && !line.contains("coordinate, data")
                && !line.contains("coordinate, prev_data")
        }),
        "presence projections contain only write-time payload indicators"
    );
}
