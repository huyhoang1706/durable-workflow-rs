//! The bare replayer answers every case in the corpus as recorded.

use durable_testkit::corpus::{self, Case};

#[test]
fn the_replayer_plays_every_case_as_recorded() {
    let cases = corpus::load_all().unwrap();
    assert!(!cases.is_empty(), "the corpus is empty");
    let failures: Vec<String> = cases
        .iter()
        .filter_map(|c| corpus::play(c).err().map(|e| format!("{}: {e}", c.name)))
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn every_file_is_canonical_and_named_after_its_case() {
    for case in corpus::load_all().unwrap() {
        let path = corpus::path(&case.name);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("case `{}` is not in {}: {e}", case.name, path.display()));
        assert_eq!(
            text,
            case.to_json(),
            "{} is not in canonical form",
            path.display()
        );
    }
}

#[test]
fn a_changed_answer_is_reported_with_its_op() {
    let mut case: Case = corpus::load_all()
        .unwrap()
        .into_iter()
        .find(|c| c.name == "sequential_steps")
        .expect("the sequential_steps case");
    let i = case
        .trace
        .iter()
        .position(|op| matches!(op, durable_sm::TraceOp::UserKey { .. }))
        .unwrap();
    if let durable_sm::TraceOp::UserKey { key, .. } = &mut case.trace[i] {
        *key = "elsewhere".into();
    }
    let err = corpus::play(&case).unwrap_err();
    assert!(err.starts_with(&format!("op #{i}:")), "{err}");
}
