use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aetherflow_core::domain::{
    AttemptId, NodeId, NodeIncarnationId, RetryPolicy, TaskId, TaskRunId, TaskStatus, WorkerId,
    WorkflowId, WorkflowRunId,
};
use aetherflow_runtime::repository::{TaskInput, TaskOutput};
use aetherflow_storage_postgres::{
    MembershipStatus, MigrationOutcome, NewTaskRun, PgMembershipRepository, PgStore,
    PgTaskRepository, StorageError, TaskFailure,
};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio::{
    io::copy_bidirectional,
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tokio_postgres::{Config, NoTls, config::Host};

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos();
    format!("{}_{}", std::process::id(), nanos)
}
/// A local TCP relay used to isolate only the child worker's PostgreSQL path.
/// Closing the gate aborts established streams and rejects new connections;
/// the test controller and replacement incarnation keep using the direct DB URL.
struct TcpDatabaseGate {
    address: SocketAddr,
    control: mpsc::UnboundedSender<(bool, oneshot::Sender<()>)>,
    task: tokio::task::JoinHandle<()>,
}

impl TcpDatabaseGate {
    async fn start(upstream_host: String, upstream_port: u16) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind isolated PostgreSQL TCP gate");
        let address = listener
            .local_addr()
            .expect("read isolated PostgreSQL TCP gate address");
        let (control, mut commands) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut available = true;
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    command = commands.recv() => {
                        match command {
                            Some((next_available, acknowledged)) => {
                                available = next_available;
                                if !available {
                                    // Closing the gate also tears down streams that
                                    // were already established before the partition.
                                    connections.abort_all();
                                    while connections.join_next().await.is_some() {}
                                }
                                let _ = acknowledged.send(());
                            }
                            None => break,
                        }
                    }
                    accepted = listener.accept() => {
                        match accepted {
                            Ok((mut client, _)) => {
                                if !available {
                                    continue;
                                }
                                let upstream_host = upstream_host.clone();
                                connections.spawn(async move {
                                    if let Ok(mut server) =
                                        TcpStream::connect((upstream_host.as_str(), upstream_port)).await
                                    {
                                        let _ = copy_bidirectional(&mut client, &mut server).await;
                                    }
                                });
                            }
                            Err(_) => break,
                        }
                    }
                    Some(_) = connections.join_next(), if !connections.is_empty() => {}
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        Self {
            address,
            control,
            task,
        }
    }

    async fn set_available(&self, available: bool) {
        let (acknowledge, acknowledged) = oneshot::channel();
        self.control
            .send((available, acknowledge))
            .expect("TCP database gate controller is still running");
        acknowledged
            .await
            .expect("TCP database gate applied requested partition state");
    }

    async fn shutdown(self) {
        drop(self.control);
        let _ = self.task.await;
    }
}

fn pool_from_url(database_url: &str, max_size: usize) -> Pool {
    let config: Config = database_url.parse().expect("valid PostgreSQL URL");
    pool_from_config(config, max_size)
}

fn pool_from_config(config: Config, max_size: usize) -> Pool {
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

    let membership = PgMembershipRepository::from_pool(pool.clone());
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
    let lease_client = pool.get().await.expect("get lease verification connection");
    let persisted_worker_expiry_is_capped: bool = lease_client
        .query_one(
            "SELECT worker.lease_expires_at <= node.lease_expires_at
             FROM worker_registrations AS worker
             JOIN cluster_nodes AS node
               ON node.node_id = worker.node_id
              AND node.membership_epoch = worker.membership_epoch
             WHERE worker.worker_id = $1 AND node.node_id = $2",
            &[&worker_id.as_str(), &node_id.as_str()],
        )
        .await
        .expect("read persisted worker/node lease relationship")
        .get(0);
    assert!(
        persisted_worker_expiry_is_capped,
        "a node heartbeat that shortens its lease must cap persisted child worker leases"
    );

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

    let pool = pool_from_url(&database_url, 6);
    PgStore::from_pool(pool.clone())
        .migrate()
        .await
        .expect("ensure the isolated membership race fixture has the latest schema");
    let membership = PgMembershipRepository::from_pool(pool);
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
    let winner = match (a, b) {
        (Ok(winner), Err(StorageError::NodeLeaseBusy(_)))
        | (Err(StorageError::NodeLeaseBusy(_)), Ok(winner)) => winner,
        (first, second) => panic!(
            "expected exactly one membership winner and one fenced loser; got {first:?} and {second:?}"
        ),
    };
    membership
        .drain_node(&winner)
        .await
        .expect("drain winning incarnation");
}
#[tokio::test]
async fn postgres_stale_node_epoch_cannot_heartbeat_or_settle_a_live_task() {
    let Ok(database_url) = std::env::var("AETHERFLOW_TEST_DATABASE_URL") else {
        eprintln!(
            "AETHERFLOW_TEST_DATABASE_URL is not set; skipping stale task-worker epoch integration test"
        );
        return;
    };

    let pool = pool_from_url(&database_url, 8);
    let store = PgStore::from_pool(pool.clone());
    assert!(matches!(
        store
            .migrate()
            .await
            .expect("ensure current PostgreSQL schema"),
        MigrationOutcome::Applied { version: 3, .. }
            | MigrationOutcome::AlreadyCurrent { version: 3, .. }
    ));
    let membership = PgMembershipRepository::from_pool(pool.clone());
    let tasks = PgTaskRepository::from_pool(pool.clone());
    let suffix = unique_suffix();

    let node_id = NodeId::parse(format!("stale-task-node-{suffix}")).expect("valid node ID");
    let old_incarnation = NodeIncarnationId::parse(format!("stale-task-old-{suffix}"))
        .expect("valid old node incarnation");
    let new_incarnation = NodeIncarnationId::parse(format!("stale-task-new-{suffix}"))
        .expect("valid new node incarnation");
    let old_worker_id =
        WorkerId::parse(format!("stale-task-worker-old-{suffix}")).expect("valid old worker ID");
    let new_worker_id =
        WorkerId::parse(format!("stale-task-worker-new-{suffix}")).expect("valid new worker ID");

    let workflow_id =
        WorkflowId::parse(format!("stale-task-wf-{suffix}")).expect("valid workflow ID");
    let workflow_run_id =
        WorkflowRunId::parse(format!("stale-task-run-{suffix}")).expect("valid workflow-run ID");
    let task_id = TaskId::parse(format!("stale-task-def-{suffix}")).expect("valid task ID");
    let task_run_id =
        TaskRunId::parse(format!("stale-task-exec-{suffix}")).expect("valid task-run ID");
    let retry_policy = RetryPolicy::new(NonZeroU32::new(2).expect("positive retry budget"), 0, 0)
        .expect("valid retry policy");
    tasks
        .create_workflow(
            &workflow_run_id,
            &workflow_id,
            None,
            &[NewTaskRun::new(
                task_run_id.clone(),
                task_id.clone(),
                TaskInput::new(b"membership-fenced-input".to_vec()).expect("valid task input"),
                Vec::new(),
                retry_policy,
                SystemTime::now() - Duration::from_secs(1),
            )],
        )
        .await
        .expect("create task for stale-worker fencing test");
    tasks
        .promote_due_tasks(8)
        .await
        .expect("promote ready task");

    // Start the short membership lease after the task fixture is ready so
    // setup time cannot consume the epoch-fencing test's active lease window.
    let old_node = membership
        .join_node(&node_id, &old_incarnation, Duration::from_secs(5))
        .await
        .expect("old node incarnation joins");
    let old_registration = membership
        .register_worker(
            &old_node,
            &old_worker_id,
            NonZeroU32::new(2).expect("positive worker capacity"),
            Duration::from_secs(4),
        )
        .await
        .expect("old worker registration succeeds");

    let attempt_id =
        AttemptId::parse(format!("stale-task-attempt-{suffix}")).expect("valid attempt ID");
    let claim = tasks
        .claim_next_registered_for_tasks(
            &old_registration,
            std::slice::from_ref(&task_id),
            &attempt_id,
            Duration::from_secs(15),
        )
        .await
        .expect("registered worker may claim while its epoch is active")
        .expect("ready task is claimed");
    assert_eq!(
        claim.task_run().status(),
        aetherflow_core::domain::TaskStatus::Running
    );

    tokio::time::sleep(Duration::from_millis(5_200)).await;
    let new_node = membership
        .join_node(&node_id, &new_incarnation, Duration::from_secs(3))
        .await
        .expect("new process takes over the expired node lease");
    assert_eq!(new_node.epoch(), old_node.epoch() + 1);
    let new_registration = membership
        .register_worker(
            &new_node,
            &new_worker_id,
            NonZeroU32::new(2).expect("positive worker capacity"),
            Duration::from_secs(20),
        )
        .await
        .expect("new worker registers under the replacement epoch");

    assert!(matches!(
        tasks
            .heartbeat_registered(
                &task_run_id,
                &old_registration,
                claim.task_run().version(),
                claim.fencing_token(),
                Duration::from_secs(15),
            )
            .await,
        Err(StorageError::NodeMembershipLost(id)) if id == node_id.as_str()
    ));
    assert!(matches!(
        tasks
            .complete_registered(
                &task_run_id,
                &old_registration,
                claim.task_run().version(),
                claim.fencing_token(),
                TaskOutput::new(b"stale-output".to_vec()).expect("valid output"),
            )
            .await,
        Err(StorageError::NodeMembershipLost(id)) if id == node_id.as_str()
    ));
    assert!(matches!(
        tasks
            .record_failure_registered(
                &task_run_id,
                &old_registration,
                claim.task_run().version(),
                claim.fencing_token(),
                TaskFailure::permanent("stale-worker", "must not settle after takeover"),
            )
            .await,
        Err(StorageError::NodeMembershipLost(id)) if id == node_id.as_str()
    ));
    assert!(matches!(
        tasks
            .timeout_task_run_registered(
                &task_run_id,
                &old_registration,
                claim.task_run().version(),
                claim.fencing_token(),
            )
            .await,
        Err(StorageError::NodeMembershipLost(id)) if id == node_id.as_str()
    ));
    assert!(matches!(
        tasks
            .cancel_task_run_registered(
                &task_run_id,
                &old_registration,
                claim.task_run().version(),
                claim.fencing_token(),
            )
            .await,
        Err(StorageError::NodeMembershipLost(id)) if id == node_id.as_str()
    ));
    assert!(matches!(
        tasks
            .recover_expired_leases_registered(&old_registration, 8)
            .await,
        Err(StorageError::NodeMembershipLost(id)) if id == node_id.as_str()
    ));
    assert!(matches!(
        tasks
            .promote_due_tasks_registered(&old_registration, 8)
            .await,
        Err(StorageError::NodeMembershipLost(id)) if id == node_id.as_str()
    ));
    assert!(matches!(
        tasks
            .aggregate_workflow_run_registered(&workflow_run_id, &old_registration)
            .await,
        Err(StorageError::NodeMembershipLost(id)) if id == node_id.as_str()
    ));

    let unchanged = tasks
        .get_task_run(&task_run_id)
        .await
        .expect("read task after stale write attempts");
    assert_eq!(
        unchanged.status(),
        aetherflow_core::domain::TaskStatus::Running
    );
    assert_eq!(unchanged.version(), claim.task_run().version());
    assert_eq!(unchanged.fencing_token(), claim.fencing_token());
    assert_eq!(
        unchanged.claimed_by().map(WorkerId::as_str),
        Some(old_worker_id.as_str())
    );

    // The replacement worker recovers the still-running task after its old
    // task lease expires. Membership takeover alone must not rewrite task state;
    // recovery is an explicit, fenced transaction owned by the new epoch.
    let lease_client = pool.get().await.expect("get task lease connection");
    lease_client
        .execute(
            "UPDATE task_runs SET lease_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE task_run_id = $1",
            &[&task_run_id.as_str()],
        )
        .await
        .expect("expire old worker task lease");

    let recovered = tasks
        .recover_expired_leases_registered(&new_registration, 8)
        .await
        .expect("replacement worker recovers old task lease");
    assert_eq!(recovered.len(), 1);
    assert_eq!(
        recovered[0].task_run().status(),
        aetherflow_core::domain::TaskStatus::RetryWaiting
    );

    tasks
        .promote_due_tasks_registered(&new_registration, 8)
        .await
        .expect("replacement worker promotes recovered retry");
    let replacement_attempt = AttemptId::parse(format!("stale-task-attempt-new-{suffix}"))
        .expect("valid replacement attempt ID");
    let replacement_claim = tasks
        .claim_next_registered_for_tasks(
            &new_registration,
            std::slice::from_ref(&task_id),
            &replacement_attempt,
            Duration::from_secs(15),
        )
        .await
        .expect("replacement worker claims recovered task")
        .expect("recovered task is ready");
    assert_eq!(replacement_claim.attempt_number().get(), 2);
    assert_eq!(replacement_claim.fencing_token(), claim.fencing_token() + 1);

    let completed = tasks
        .complete_registered(
            &task_run_id,
            &new_registration,
            replacement_claim.task_run().version(),
            replacement_claim.fencing_token(),
            TaskOutput::new(b"recovered-by-new-epoch".to_vec()).expect("valid output"),
        )
        .await
        .expect("replacement epoch completes recovered task");
    assert_eq!(
        completed.status(),
        aetherflow_core::domain::TaskStatus::Succeeded
    );

    membership
        .drain_node(&new_node)
        .await
        .expect("drain replacement test node");
    assert_eq!(new_registration.node().epoch(), new_node.epoch());
}

#[tokio::test]
async fn postgres_registered_claim_filters_by_worker_task_capabilities() {
    let Ok(database_url) = std::env::var("AETHERFLOW_TEST_DATABASE_URL") else {
        eprintln!(
            "AETHERFLOW_TEST_DATABASE_URL is not set; skipping PostgreSQL worker capability test"
        );
        return;
    };

    let pool = pool_from_url(&database_url, 6);
    let store = PgStore::from_pool(pool.clone());
    store
        .migrate()
        .await
        .expect("ensure current PostgreSQL schema");
    let membership = PgMembershipRepository::from_pool(pool.clone());
    let tasks = PgTaskRepository::from_pool(pool);
    let suffix = unique_suffix();

    let workflow_id =
        WorkflowId::parse(format!("capability-workflow-{suffix}")).expect("valid workflow ID");
    let workflow_run_id =
        WorkflowRunId::parse(format!("capability-run-{suffix}")).expect("valid workflow run ID");
    let task_id = TaskId::parse(format!("capability-task-{suffix}")).expect("valid task ID");
    let task_run_id =
        TaskRunId::parse(format!("capability-task-run-{suffix}")).expect("valid task run ID");
    let retry_policy = RetryPolicy::new(NonZeroU32::new(1).expect("positive attempt budget"), 0, 0)
        .expect("valid retry policy");

    tasks
        .create_workflow(
            &workflow_run_id,
            &workflow_id,
            None,
            &[NewTaskRun::new(
                task_run_id.clone(),
                task_id.clone(),
                TaskInput::empty(),
                Vec::new(),
                retry_policy,
                SystemTime::now() - Duration::from_secs(1),
            )],
        )
        .await
        .expect("create capability-filtered task");
    tasks
        .promote_due_tasks(8)
        .await
        .expect("promote capability-filtered task");

    let node_id = NodeId::parse(format!("capability-node-{suffix}")).expect("valid node ID");
    let incarnation = NodeIncarnationId::parse(format!("capability-incarnation-{suffix}"))
        .expect("valid node incarnation");
    let worker_id =
        WorkerId::parse(format!("capability-worker-{suffix}")).expect("valid worker ID");
    let node = membership
        .join_node(&node_id, &incarnation, Duration::from_secs(60))
        .await
        .expect("join capability-test node");
    let registration = membership
        .register_worker(
            &node,
            &worker_id,
            NonZeroU32::new(1).expect("positive worker capacity"),
            Duration::from_secs(30),
        )
        .await
        .expect("register capability-test worker");

    let empty_registry_attempt =
        AttemptId::parse(format!("capability-empty-attempt-{suffix}")).expect("valid attempt ID");
    assert!(
        tasks
            .claim_next_registered_for_tasks(
                &registration,
                &[],
                &empty_registry_attempt,
                Duration::from_secs(10),
            )
            .await
            .expect("empty capability set should be a valid no-op claim")
            .is_none(),
        "worker with no registered handlers must not claim any task"
    );
    assert_eq!(
        tasks
            .get_task_run(&task_run_id)
            .await
            .expect("read task after empty capability claim")
            .status(),
        TaskStatus::Ready,
        "unsupported task must remain ready for a capable worker"
    );

    let supported_attempt = AttemptId::parse(format!("capability-supported-attempt-{suffix}"))
        .expect("valid attempt ID");
    let claim = tasks
        .claim_next_registered_for_tasks(
            &registration,
            std::slice::from_ref(&task_id),
            &supported_attempt,
            Duration::from_secs(10),
        )
        .await
        .expect("capable worker claims matching task")
        .expect("matching task remains available");
    assert_eq!(claim.task_run().task_id(), &task_id);
    let completed = tasks
        .complete_registered(
            &task_run_id,
            &registration,
            claim.task_run().version(),
            claim.fencing_token(),
            TaskOutput::empty(),
        )
        .await
        .expect("capable worker completes its task");
    assert_eq!(completed.status(), TaskStatus::Succeeded);

    membership
        .drain_node(&node)
        .await
        .expect("drain capability-test node");
}

#[tokio::test]
async fn postgres_database_partition_then_epoch_takeover_recovers_task() {
    let Ok(database_url) = std::env::var("AETHERFLOW_TEST_DATABASE_URL") else {
        eprintln!(
            "AETHERFLOW_TEST_DATABASE_URL is not set; skipping database-partition recovery test"
        );
        return;
    };

    let admin_pool = pool_from_url(&database_url, 8);
    let store = PgStore::from_pool(admin_pool.clone());
    store
        .migrate()
        .await
        .expect("ensure current PostgreSQL schema");
    let admin_membership = PgMembershipRepository::from_pool(admin_pool.clone());
    let admin_tasks = PgTaskRepository::from_pool(admin_pool.clone());
    let suffix = unique_suffix();

    let workflow_id =
        WorkflowId::parse(format!("partition-workflow-{suffix}")).expect("valid workflow ID");
    let workflow_run_id =
        WorkflowRunId::parse(format!("partition-run-{suffix}")).expect("valid workflow run ID");
    let task_id = TaskId::parse(format!("partition-task-{suffix}")).expect("valid task ID");
    let task_run_id =
        TaskRunId::parse(format!("partition-task-run-{suffix}")).expect("valid task run ID");
    let node_id = NodeId::parse(format!("partition-node-{suffix}")).expect("valid node ID");
    let old_incarnation = NodeIncarnationId::parse(format!("partition-old-incarnation-{suffix}"))
        .expect("valid old incarnation");
    let new_incarnation = NodeIncarnationId::parse(format!("partition-new-incarnation-{suffix}"))
        .expect("valid new incarnation");
    let old_worker_id =
        WorkerId::parse(format!("partition-old-worker-{suffix}")).expect("valid old worker ID");
    let new_worker_id =
        WorkerId::parse(format!("partition-new-worker-{suffix}")).expect("valid new worker ID");
    let retry_policy = RetryPolicy::new(NonZeroU32::new(2).expect("positive retry budget"), 0, 0)
        .expect("valid retry policy");

    admin_tasks
        .create_workflow(
            &workflow_run_id,
            &workflow_id,
            None,
            &[NewTaskRun::new(
                task_run_id.clone(),
                task_id.clone(),
                TaskInput::new(b"partition-recovery-input".to_vec()).expect("valid task input"),
                Vec::new(),
                retry_policy,
                SystemTime::now() - Duration::from_secs(1),
            )],
        )
        .await
        .expect("create task before database partition");
    admin_tasks
        .promote_due_tasks(8)
        .await
        .expect("promote partition test task");

    // Use a dedicated database role for the old process so its DB access can
    // be cut without interrupting the replacement node or the test controller.
    // The role name is generated from numeric process/time data and is quoted.
    let role_name = format!("awp_{}", &suffix[suffix.len() - 24..]);
    let quoted_role = format!("\"{role_name}\"");
    let role_password = format!("ephemeral-{suffix}-credential");
    let admin_client = admin_pool
        .get()
        .await
        .expect("get admin connection for isolated worker role");
    // The generated credential is restricted to a safe ASCII alphabet, so
    // it can be embedded as a SQL literal in PostgreSQL's CREATE ROLE command
    // (utility statements do not accept bind parameters for this option).
    admin_client
        .batch_execute(&format!(
            "CREATE ROLE {quoted_role} LOGIN PASSWORD '{role_password}';"
        ))
        .await
        .expect("create temporary isolated worker role");
    admin_client
        .batch_execute(&format!(
            "GRANT USAGE ON SCHEMA public TO {quoted_role};
             GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {quoted_role};
             GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {quoted_role};"
        ))
        .await
        .expect("grant existing storage tables to isolated worker");
    let database_name: String = admin_client
        .query_one("SELECT current_database()", &[])
        .await
        .expect("read test database name")
        .get(0);
    let quoted_database = format!("\"{}\"", database_name.replace('"', "\"\""));
    admin_client
        .batch_execute(&format!(
            "GRANT CONNECT ON DATABASE {quoted_database} TO {quoted_role};"
        ))
        .await
        .expect("grant isolated worker database connection");

    // Route only the old process through a TCP relay. The parent/replacement
    // continue to use the direct database endpoint, so closing the relay
    // partitions one worker without interrupting the controller.
    let database_config: Config = database_url.parse().expect("valid PostgreSQL URL");
    let upstream_host = match database_config.get_hosts().first() {
        Some(Host::Tcp(host)) => host.clone(),
        Some(Host::Unix(_)) => panic!("TCP partition test requires a TCP PostgreSQL endpoint"),
        None => panic!("TCP partition test requires an explicit PostgreSQL host"),
    };
    let upstream_port = database_config.get_ports().first().copied().unwrap_or(5432);
    let database_gate = TcpDatabaseGate::start(upstream_host, upstream_port).await;
    let mut child_database_config = Config::new();
    child_database_config
        .user(&role_name)
        .password(&role_password)
        .dbname(&database_name)
        .host("127.0.0.1")
        .port(database_gate.address.port())
        .connect_timeout(Duration::from_secs(2));
    let child_database_url = child_database_config.to_string();

    // Run the old incarnation in a distinct OS process. It holds the claim
    // and registration in its own address space while the TCP relay is cut.
    let ready_path = std::env::temp_dir().join(format!("aetherflow-partition-{suffix}.ready"));
    let partition_path =
        std::env::temp_dir().join(format!("aetherflow-partition-{suffix}.partition"));
    let partition_ack_path =
        std::env::temp_dir().join(format!("aetherflow-partition-{suffix}.partition-ack"));
    let resume_path = std::env::temp_dir().join(format!("aetherflow-partition-{suffix}.resume"));
    let _ = std::fs::remove_file(&ready_path);
    let _ = std::fs::remove_file(&partition_path);
    let _ = std::fs::remove_file(&partition_ack_path);
    let _ = std::fs::remove_file(&resume_path);
    let child_executable = std::env::current_exe().expect("current integration-test executable");
    let mut old_worker_process = std::process::Command::new(child_executable)
        .args([
            "--exact",
            "postgres_partition_old_worker_child_process",
            "--nocapture",
        ])
        .env(
            "AETHERFLOW_PARTITION_CHILD_DATABASE_URL",
            &child_database_url,
        )
        .env("AETHERFLOW_PARTITION_CHILD_NODE_ID", node_id.as_str())
        .env(
            "AETHERFLOW_PARTITION_CHILD_INCARNATION_ID",
            old_incarnation.as_str(),
        )
        .env(
            "AETHERFLOW_PARTITION_CHILD_WORKER_ID",
            old_worker_id.as_str(),
        )
        .env("AETHERFLOW_PARTITION_CHILD_TASK_ID", task_id.as_str())
        .env(
            "AETHERFLOW_PARTITION_CHILD_TASK_RUN_ID",
            task_run_id.as_str(),
        )
        .env(
            "AETHERFLOW_PARTITION_CHILD_ATTEMPT_ID",
            format!("partition-old-attempt-{suffix}"),
        )
        .env("AETHERFLOW_PARTITION_CHILD_READY_PATH", &ready_path)
        .env("AETHERFLOW_PARTITION_CHILD_PARTITION_PATH", &partition_path)
        .env(
            "AETHERFLOW_PARTITION_CHILD_PARTITION_ACK_PATH",
            &partition_ack_path,
        )
        .env("AETHERFLOW_PARTITION_CHILD_RESUME_PATH", &resume_path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .expect("spawn old worker in a separate OS process");

    let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !ready_path.exists() && tokio::time::Instant::now() < ready_deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    if !ready_path.exists() {
        let _ = old_worker_process.kill();
        let _ = old_worker_process.wait();
        panic!("old worker process did not claim its task before timeout");
    }
    let old_epoch: u64 = std::fs::read_to_string(&ready_path)
        .expect("read old worker's process-local membership epoch")
        .trim()
        .parse()
        .expect("old worker writes its node epoch after claiming");

    // Sever the child process's TCP path. The gate aborts all established
    // streams and rejects new connections while the parent stays connected
    // directly to PostgreSQL.
    database_gate.set_available(false).await;
    std::fs::write(&partition_path, "partition")
        .expect("signal the child process that PostgreSQL access is partitioned");
    let partition_ack_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !partition_ack_path.exists() && tokio::time::Instant::now() < partition_ack_deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        partition_ack_path.exists(),
        "old worker process did not fail closed while its PostgreSQL TCP path was severed"
    );

    tokio::time::sleep(Duration::from_millis(3_300)).await;
    let new_node = admin_membership
        .join_node(&node_id, &new_incarnation, Duration::from_secs(30))
        .await
        .expect("replacement incarnation takes over after old process lease expiry");
    assert_eq!(new_node.epoch(), old_epoch + 1);
    let new_registration = admin_membership
        .register_worker(
            &new_node,
            &new_worker_id,
            NonZeroU32::new(1).expect("positive worker capacity"),
            Duration::from_secs(20),
        )
        .await
        .expect("replacement worker registers under new epoch");

    // The new epoch owns recovery after the old task lease expires. The old
    // claim remains fenced even though the database connection is restored.
    let lease_client = admin_pool
        .get()
        .await
        .expect("get admin task-lease connection");
    lease_client
        .execute(
            "UPDATE task_runs
             SET lease_expires_at = clock_timestamp() - INTERVAL '1 second'
             WHERE task_run_id = $1 AND status = 'running'",
            &[&task_run_id.as_str()],
        )
        .await
        .expect("expire disconnected worker task lease");
    let old_fencing_token: i64 = lease_client
        .query_one(
            "SELECT fencing_token FROM task_runs WHERE task_run_id = $1",
            &[&task_run_id.as_str()],
        )
        .await
        .expect("read old worker fencing token")
        .get(0);
    let recovered = admin_tasks
        .recover_expired_leases_registered(&new_registration, 8)
        .await
        .expect("replacement epoch recovers disconnected worker task");
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].task_run().status(), TaskStatus::RetryWaiting);
    admin_tasks
        .promote_due_tasks_registered(&new_registration, 8)
        .await
        .expect("replacement epoch promotes recovered task");
    let new_attempt =
        AttemptId::parse(format!("partition-new-attempt-{suffix}")).expect("valid new attempt ID");
    let new_claim = admin_tasks
        .claim_next_registered_for_tasks(
            &new_registration,
            std::slice::from_ref(&task_id),
            &new_attempt,
            Duration::from_secs(10),
        )
        .await
        .expect("replacement epoch claims recovered task")
        .expect("recovered task is ready");
    assert_eq!(new_claim.attempt_number().get(), 2);
    assert!(new_claim.fencing_token() > old_fencing_token as u64);
    // Reconnect the old OS process while the replacement owns attempt 2.
    // Both stale heartbeat and stale completion must be fenced before the new
    // owner is allowed to settle the task.
    database_gate.set_available(true).await;
    std::fs::write(&resume_path, "resume")
        .expect("signal old worker process to reconnect while replacement owns task");
    let child_status = tokio::task::spawn_blocking(move || {
        old_worker_process
            .wait()
            .expect("wait for old worker process after reconnect")
    })
    .await
    .expect("join old worker process wait task");
    assert!(
        child_status.success(),
        "old worker process failed to observe stale-epoch fencing: {child_status}"
    );
    let _ = std::fs::remove_file(&ready_path);
    let _ = std::fs::remove_file(&partition_path);
    let _ = std::fs::remove_file(&partition_ack_path);
    let _ = std::fs::remove_file(&resume_path);

    let completed = admin_tasks
        .complete_registered(
            &task_run_id,
            &new_registration,
            new_claim.task_run().version(),
            new_claim.fencing_token(),
            TaskOutput::new(b"recovered-after-database-partition".to_vec())
                .expect("valid recovered output"),
        )
        .await
        .expect("replacement worker completes recovered task");
    assert_eq!(completed.status(), TaskStatus::Succeeded);

    admin_membership
        .drain_node(&new_node)
        .await
        .expect("drain replacement partition-test node");
    database_gate.shutdown().await;

    // Release the temporary role and its grants after the child process exits.
    let _ = admin_client
        .query(
            "SELECT pg_terminate_backend(pid)
             FROM pg_stat_activity
             WHERE usename = $1 AND pid <> pg_backend_pid()",
            &[&role_name],
        )
        .await
        .expect("terminate temporary worker role sessions");
    admin_client
        .batch_execute(&format!(
            "DROP OWNED BY {quoted_role};
             DROP ROLE {quoted_role};"
        ))
        .await
        .expect("remove temporary isolated worker role");
}

/// Separate OS process for the database-partition regression. It owns the old
/// membership registration and task claim while the parent replaces its epoch.
#[tokio::test]
async fn postgres_partition_old_worker_child_process() {
    let (
        Ok(database_url),
        Ok(node_id),
        Ok(incarnation_id),
        Ok(worker_id),
        Ok(task_id),
        Ok(task_run_id),
        Ok(attempt_id),
        Ok(ready_path),
        Ok(partition_path),
        Ok(partition_ack_path),
        Ok(resume_path),
    ) = (
        std::env::var("AETHERFLOW_PARTITION_CHILD_DATABASE_URL"),
        std::env::var("AETHERFLOW_PARTITION_CHILD_NODE_ID"),
        std::env::var("AETHERFLOW_PARTITION_CHILD_INCARNATION_ID"),
        std::env::var("AETHERFLOW_PARTITION_CHILD_WORKER_ID"),
        std::env::var("AETHERFLOW_PARTITION_CHILD_TASK_ID"),
        std::env::var("AETHERFLOW_PARTITION_CHILD_TASK_RUN_ID"),
        std::env::var("AETHERFLOW_PARTITION_CHILD_ATTEMPT_ID"),
        std::env::var("AETHERFLOW_PARTITION_CHILD_READY_PATH"),
        std::env::var("AETHERFLOW_PARTITION_CHILD_PARTITION_PATH"),
        std::env::var("AETHERFLOW_PARTITION_CHILD_PARTITION_ACK_PATH"),
        std::env::var("AETHERFLOW_PARTITION_CHILD_RESUME_PATH"),
    )
    else {
        return;
    };

    let child_config: Config = database_url.parse().expect("valid child PostgreSQL URL");
    let pool = pool_from_config(child_config, 4);
    let membership = PgMembershipRepository::from_pool(pool.clone());
    let tasks = PgTaskRepository::from_pool(pool.clone());
    let node_id = NodeId::parse(node_id).expect("valid child node ID");
    let incarnation_id =
        NodeIncarnationId::parse(incarnation_id).expect("valid child incarnation ID");
    let worker_id = WorkerId::parse(worker_id).expect("valid child worker ID");
    let task_id = TaskId::parse(task_id).expect("valid child task ID");
    let task_run_id = TaskRunId::parse(task_run_id).expect("valid child task-run ID");
    let attempt_id = AttemptId::parse(attempt_id).expect("valid child attempt ID");

    let node = membership
        .join_node(&node_id, &incarnation_id, Duration::from_secs(3))
        .await
        .expect("old child node joins");
    let registration = membership
        .register_worker(
            &node,
            &worker_id,
            NonZeroU32::new(1).expect("positive worker capacity"),
            Duration::from_secs(2),
        )
        .await
        .expect("old child worker registers");
    let claim = tasks
        .claim_next_registered_for_tasks(
            &registration,
            std::slice::from_ref(&task_id),
            &attempt_id,
            Duration::from_secs(10),
        )
        .await
        .expect("old child worker claims task before partition")
        .expect("partition task should be claimed by old child");
    std::fs::write(&ready_path, node.epoch().to_string())
        .expect("publish child readiness after durable claim");

    tokio::time::timeout(Duration::from_secs(10), async {
        while !std::path::Path::new(&partition_path).exists() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("parent should signal the database partition");

    assert!(
        tasks
            .heartbeat_registered(
                &task_run_id,
                &registration,
                claim.task_run().version(),
                claim.fencing_token(),
                Duration::from_secs(10),
            )
            .await
            .is_err(),
        "old worker must fail closed while its PostgreSQL TCP path is severed"
    );
    std::fs::write(&partition_ack_path, "database-error-observed")
        .expect("acknowledge fail-closed behavior to parent");

    tokio::time::timeout(Duration::from_secs(20), async {
        while !std::path::Path::new(&resume_path).exists() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("parent should restore the PostgreSQL TCP path and resume the old process");

    assert!(matches!(
        tasks
            .heartbeat_registered(
                &task_run_id,
                &registration,
                claim.task_run().version(),
                claim.fencing_token(),
                Duration::from_secs(10),
            )
            .await,
        Err(StorageError::NodeMembershipLost(id)) if id == node_id.as_str()
    ));
    assert!(matches!(
        tasks
            .complete_registered(
                &task_run_id,
                &registration,
                claim.task_run().version(),
                claim.fencing_token(),
                TaskOutput::new(b"stale-partition-output".to_vec())
                    .expect("valid stale output"),
            )
            .await,
        Err(StorageError::NodeMembershipLost(id)) if id == node_id.as_str()
    ));
    drop(tasks);
    drop(membership);
    drop(pool);
}
