use super::*;

#[tokio::test]
async fn fresh_database_reaches_max_schema_version() {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();

    let current: i32 = sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM schema_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(current, MAX_SCHEMA_VERSION);
}

#[tokio::test]
async fn migrations_idempotent_when_schema_version_rewound() {
    // Simulates the #112 failure mode: a prior interrupted run left the
    // schema fully advanced but schema_version was rolled back to an older
    // value. Re-running migrations must succeed — ALTER TABLE ADD COLUMN
    // statements must no-op when the column already exists.
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();

    // Simulate rolled-back schema_version while schema is intact.
    sqlx::query("DELETE FROM schema_version WHERE version > 4")
        .execute(&pool)
        .await
        .unwrap();
    let v: i32 = sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM schema_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(v, 4, "precondition: schema_version rewound to 4");

    // Without the fix, V5 fails with 'duplicate column name: auth_token'.
    run_migrations(&pool).await.unwrap();

    let v: i32 = sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM schema_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(v, MAX_SCHEMA_VERSION);
}
