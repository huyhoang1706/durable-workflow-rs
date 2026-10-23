//! Misuse of `#[workflow]` / `#[step]` must produce pointed compile errors.
//! Regenerate the expected output with `TRYBUILD=overwrite cargo test -p durable-workflow --test compile_fail`
//! and review the diff.

#[test]
fn macro_misuse_reports_clear_errors() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
