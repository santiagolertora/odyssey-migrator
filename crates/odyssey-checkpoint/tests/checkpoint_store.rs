//! Public-API smoke tests for `ferry-checkpoint` (external consumer view).

use odyssey_checkpoint::{CheckpointStore, NewMigration, UnitProgress};
use odyssey_types::{MigrationState, MigrationUnit, TokenRange};
use tempfile::TempDir;

#[test]
fn end_to_end_claim_progress_complete() {
    let dir = TempDir::new().unwrap();
    let store = CheckpointStore::open(dir.path().join("ferry.db")).unwrap();

    let migration_id = store
        .create_migration(NewMigration {
            name: "e2e".into(),
            source_cluster: "a".into(),
            target_cluster: "b".into(),
            keyspace_name: "ks".into(),
            table_name: "t".into(),
            schema_hash: "h".into(),
        })
        .unwrap();

    let unit =
        MigrationUnit::new("ks", "t", TokenRange::new(0, 100).unwrap()).unwrap();
    store.insert_units(&migration_id, &[unit]).unwrap();

    let claimed = store
        .claim_pending_unit(&migration_id)
        .unwrap()
        .expect("unit");
    store
        .save_progress(UnitProgress {
            unit_id: claimed.id.clone(),
            paging_state: Some(b"page".to_vec()),
            rows_read: 3,
            rows_written: 3,
            bytes_written: 30,
            state: MigrationState::Running,
            last_token: None,
        })
        .unwrap();
    store.mark_completed(&claimed.id).unwrap();

    let summary = store.migration_summary(&migration_id).unwrap();
    assert_eq!(summary.completed, 1);
    assert!(summary.all_completed());
    assert!(store
        .mark_migration_completed_if_done(&migration_id)
        .unwrap());
}
