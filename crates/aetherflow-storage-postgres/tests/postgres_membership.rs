use std::num::{NonZeroU32, NonZeroUsize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aetherflow_core::domain::{NodeId, NodeIncarnationId, WorkerId};
use aetherflow_storage_postgres::{
    MembershipStatus, MigrationOutcome, PgMembershipRepository, PgStore, StorageError,
};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::{Config, NoTls};

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos();
    format!("{}_{}", std::process::id(), nanos)
}

fn pool_from_url(database_url: &str, max_size: usize) -> Pool {
    let config: Config = database_url.parse().expect("valid PostgreSQL URL");
    let manager = Manager::from_config(
        config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    Pool::builder(manager)
        .max_size(
            NonZeroUsize::new(max_size)
                .expect("positive PostgreSQL pool size")
                .get(),
        )
        .build()
        .expect("build PostgreSQL pool")
}

#[tokio::test]
async fn postgres_node_membership_fences_incarnations_and_drains_workers() {
    let Ok(database_url) = std::env::var("AETHERFLOW_TEST_DATABASE_URL") else {
        eprintln!(
            "AETHERFLOW_TEST_DATABASE_URL is not set; skipping PostgreSQL membership integration test"
        );
        return;
    };

    let pool = pool_from_url(&database_url, 8);
    let store = PgStore::from_pool(pool.clone());
    assert!(matches!(
        store
            .migrate()
            .await
            .expect("apply current PostgreSQL schema"),
        MigrationOutcome::Applied { version: 3, .. }
            | MigrationOutcome::AlreadyCurrent { version: 3, .. }
    ));

    let membership = PgMembershipRepository::from_pool(pool);
    let suffix = unique_suffix();
    let node_id = NodeId::parse(format!("membership-node-{suffix}")).expect("valid node ID");
    let incarnation_a = NodeIncarnationId::parse(format!("membership-incarnation-a-{suffix}"))
        .expect("valid first incarnation ID");
    let incarnation_b = NodeIncarnationId::parse(format!("membership-incarnation-b-{suffix}"))
        .expect("valid second incarnation ID");
    let worker_id =
        WorkerId::parse(format!("membership-worker-a-{suffix}")).expect("valid worker ID");
    let worker_b_id =
        WorkerId::parse(format!("membership-worker-b-{suffix}")).expect("valid second worker ID");

    let node_a = membership
        .join_node(&node_id, &incarnation_a, Duration::from_secs(3))
        .await
        .expect("first incarnation should acquire node membership");
    assert_eq!(node_a.epoch(), 1);
    assert_eq!(node_a.status(), MembershipStatus::Active);

    // The same live process incarnation may idempotently refresh its lease
    // without changing the fencing epoch.
    let node_a_renewed = membership
        .join_node(&node_id, &incarnation_a, Duration::from_secs(3))
        .await
        .expect("same incarnation may renew through join");
    assert_eq!(node_a_renewed.epoch(), 1);
    assert!(node_a_renewed.lease_expires_at() >= node_a.lease_expires_at());

    // A different process cannot steal a still-live logical node identity.
    assert!(matches!(
        membership
            .join_node(&node_id, &incarnation_b, Duration::from_secs(2))
            .await,
        Err(StorageError::NodeLeaseBusy(id)) if id == node_id.as_str()
    ));

    let worker_a = membership
        .register_worker(
            &node_a_renewed,
            &worker_id,
            NonZeroU32::new(4).expect("positive worker concurrency"),
            Duration::from_millis(250),
        )
        .await
        .expect("register worker under live active node");
    assert_eq!(worker_a.node().epoch(), 1);
    assert_eq!(worker_a.status(), MembershipStatus::Active);
    assert_eq!(worker_a.concurrency_limit().get(), 4);
    assert!(worker_a.lease_expires_at() <= node_a_renewed.lease_expires_at());

    let workers = membership
        .active_workers()
        .await
        .expect("list active workers");
    assert!(
        workers
            .iter()
            .any(|worker| worker.worker_id() == &worker_id)
    );

    // Worker and node heartbeat operations keep leases bounded by the
    // database-owned parent lease, even if the requested worker lease is longer.
    let worker_a_renewed = membership
        .heartbeat_worker(&worker_a, Duration::from_secs(5))
        .await
        .expect("renew worker registration");
    assert!(worker_a_renewed.lease_expires_at() <= node_a_renewed.lease_expires_at());

    let worker_a_draining = membership
        .drain_worker(&worker_a_renewed)
        .await
        .expect("drain the worker");
    assert_eq!(worker_a_draining.status(), MembershipStatus::Draining);
    // Node drain atomically drains all active registrations. Draining leases
    // can still heartbeat while the process completes existing work.
    let node_a_draining = membership
        .drain_node(&node_a_renewed)
        .await
        .expect("drain the node and its workers");
    assert_eq!(node_a_draining.status(), MembershipStatus::Draining);
    let worker_a_after_drain = membership
        .heartbeat_worker(&worker_a_draining, Duration::from_secs(5))
        .await
        .expect("draining worker may renew while work is finishing");
    assert_eq!(worker_a_after_drain.status(), MembershipStatus::Draining);
    assert!(matches!(
        membership
            .register_worker(
                &node_a_draining,
                &WorkerId::parse(format!("membership-worker-blocked-{suffix}"))
                    .expect("valid blocked worker ID"),
                NonZeroU32::new(1).expect("positive worker concurrency"),
                Duration::from_secs(1),
            )
            .await,
        Err(StorageError::NodeMembershipLost(id)) if id == node_id.as_str()
    ));

    let node_a_draining_renewed = membership
        .heartbeat_node(&node_a_draining, Duration::from_millis(250))
        .await
        .expect("draining node may heartbeat during graceful shutdown");
    assert_eq!(node_a_draining_renewed.status(), MembershipStatus::Draining);

    // A fresh process may take over only after the prior database-owned node
    // lease expires. The membership epoch advances, fencing every old handle.
    tokio::time::sleep(Duration::from_millis(280)).await;
    let node_b = membership
        .join_node(&node_id, &incarnation_b, Duration::from_secs(2))
        .await
        .expect("new incarnation may take over expired node membership");
    assert_eq!(node_b.epoch(), 2);
    assert_eq!(node_b.status(), MembershipStatus::Active);

    assert!(matches!(
        membership.heartbeat_node(&node_a_draining_renewed, Duration::from_secs(1)).await,
        Err(StorageError::NodeMembershipLost(id)) if id == node_id.as_str()
    ));
    assert!(matches!(
        membership
            .heartbeat_worker(&worker_a_after_drain, Duration::from_secs(1))
            .await,
        Err(StorageError::WorkerRegistrationLost(id)) if id == worker_id.as_str()
    ));

    // New workers attach to the new epoch; old worker rows are invisible to
    // active-worker discovery because their epoch no longer matches the node.
    let worker_b = membership
        .register_worker(
            &node_b,
            &worker_b_id,
            NonZeroU32::new(2).expect("positive worker concurrency"),
            Duration::from_secs(1),
        )
        .await
        .expect("register worker under replacement node incarnation");
    assert_eq!(worker_b.node().epoch(), 2);
    let active = membership
        .active_workers()
        .await
        .expect("list active workers after takeover");
    assert!(
        active
            .iter()
            .any(|worker| worker.worker_id() == &worker_b_id)
    );
    assert!(!active.iter().any(|worker| worker.worker_id() == &worker_id));

    let node_b_drained = membership
        .drain_node(&node_b)
        .await
        .expect("cleanup test node");
    assert_eq!(node_b_drained.status(), MembershipStatus::Draining);
}

#[tokio::test]
async fn postgres_concurrent_node_join_has_one_winning_incarnation() {
    let Ok(database_url) = std::env::var("AETHERFLOW_TEST_DATABASE_URL") else {
        eprintln!(
            "AETHERFLOW_TEST_DATABASE_URL is not set; skipping concurrent membership integration test"
        );
        return;
    };

    let membership = PgMembershipRepository::from_pool(pool_from_url(&database_url, 6));
    let suffix = unique_suffix();
    let node_id =
        NodeId::parse(format!("membership-race-node-{suffix}")).expect("valid racing node ID");
    let incarnation_a = NodeIncarnationId::parse(format!("membership-race-a-{suffix}"))
        .expect("valid first racing incarnation ID");
    let incarnation_b = NodeIncarnationId::parse(format!("membership-race-b-{suffix}"))
        .expect("valid second racing incarnation ID");

    let (a, b) = tokio::join!(
        membership.join_node(&node_id, &incarnation_a, Duration::from_secs(2)),
        membership.join_node(&node_id, &incarnation_b, Duration::from_secs(2)),
    );
    let outcomes = [a, b];
    let winners = outcomes.iter().filter(|outcome| outcome.is_ok()).count();
    let busy = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, Err(StorageError::NodeLeaseBusy(_))))
        .count();
    assert_eq!(winners, 1, "exactly one incarnation must acquire the node");
    assert_eq!(busy, 1, "the concurrent losing incarnation must be fenced");

    let winner = outcomes
        .into_iter()
        .find_map(Result::ok)
        .expect("one incarnation must win the membership race");
    membership
        .drain_node(&winner)
        .await
        .expect("drain winning incarnation");
}
