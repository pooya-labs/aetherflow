//! PostgreSQL-backed node membership and worker registration leases.
//!
//! Membership is an eligibility/liveness gate. Per-task leases and fencing
//! tokens remain the final guard for durable task execution.

use std::num::NonZeroU32;
use std::time::{Duration, SystemTime};

use aetherflow_core::domain::{NodeId, NodeIncarnationId, WorkerId};
use deadpool_postgres::Pool;

use crate::StorageError;

const MAX_LEASE_MILLIS: u128 = i64::MAX as u128;

/// Current lifecycle state of a node or worker membership lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MembershipStatus {
    /// May admit new worker registrations or task claims.
    Active,
    /// Stops admitting new work while allowing existing work to drain.
    Draining,
}

impl MembershipStatus {
    fn parse(value: &str) -> Result<Self, StorageError> {
        match value {
            "active" => Ok(Self::Active),
            "draining" => Ok(Self::Draining),
            other => Err(StorageError::InvalidStoredValue(format!(
                "unexpected membership status: {other}"
            ))),
        }
    }
}

/// The current fenced lease held by one node process incarnation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeMembership {
    node_id: NodeId,
    incarnation_id: NodeIncarnationId,
    epoch: u64,
    status: MembershipStatus,
    lease_expires_at: SystemTime,
}

impl NodeMembership {
    /// Returns the stable logical node identifier.
    #[must_use]
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Returns the process-specific incarnation identifier.
    #[must_use]
    pub fn incarnation_id(&self) -> &NodeIncarnationId {
        &self.incarnation_id
    }

    /// Returns the monotonically increasing membership epoch.
    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Returns the current node lifecycle state.
    #[must_use]
    pub const fn status(&self) -> MembershipStatus {
        self.status
    }

    /// Returns the lease expiry assigned by PostgreSQL.
    #[must_use]
    pub const fn lease_expires_at(&self) -> SystemTime {
        self.lease_expires_at
    }
}

/// A worker registration fenced by its parent node incarnation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerRegistration {
    worker_id: WorkerId,
    node: NodeMembership,
    concurrency_limit: NonZeroU32,
    status: MembershipStatus,
    lease_expires_at: SystemTime,
}

impl WorkerRegistration {
    /// Returns the worker process identity.
    #[must_use]
    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    /// Returns the node membership that owns this worker registration.
    #[must_use]
    pub fn node(&self) -> &NodeMembership {
        &self.node
    }

    /// Returns the maximum local task concurrency advertised by this worker.
    #[must_use]
    pub const fn concurrency_limit(&self) -> NonZeroU32 {
        self.concurrency_limit
    }

    /// Returns the worker registration lifecycle state.
    #[must_use]
    pub const fn status(&self) -> MembershipStatus {
        self.status
    }

    /// Returns the worker lease expiry assigned by PostgreSQL.
    #[must_use]
    pub const fn lease_expires_at(&self) -> SystemTime {
        self.lease_expires_at
    }
}

/// PostgreSQL repository for node and worker membership leases.
#[derive(Clone)]
pub struct PgMembershipRepository {
    pool: Pool,
}

impl PgMembershipRepository {
    /// Creates a membership repository from a caller-configured pool.
    #[must_use]
    pub fn from_pool(pool: Pool) -> Self {
        Self { pool }
    }

    /// Acquires or renews a node membership lease.
    ///
    /// Repeated acquisition by the same active incarnation is idempotent and
    /// preserves its epoch. A different incarnation may take over only after
    /// the previous node lease expires; takeover increments the epoch.
    pub async fn join_node(
        &self,
        node_id: &NodeId,
        incarnation_id: &NodeIncarnationId,
        lease_duration: Duration,
    ) -> Result<NodeMembership, StorageError> {
        let lease_millis = lease_millis(lease_duration)?;
        let client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let row = client
            .query_opt(
                "INSERT INTO cluster_nodes AS existing (
                     node_id, incarnation_id, membership_epoch, status,
                     lease_expires_at, heartbeat_at, created_at, updated_at
                 )
                 VALUES (
                     $1, $2, 1, 'active',
                     clock_timestamp() + ($3::BIGINT * INTERVAL '1 millisecond'),
                     clock_timestamp(), clock_timestamp(), clock_timestamp()
                 )
                 ON CONFLICT (node_id) DO UPDATE
                 SET incarnation_id = EXCLUDED.incarnation_id,
                     membership_epoch = CASE
                         WHEN existing.incarnation_id = EXCLUDED.incarnation_id
                         THEN existing.membership_epoch
                         ELSE existing.membership_epoch + 1
                     END,
                     status = 'active',
                     lease_expires_at =
                         clock_timestamp() + ($3::BIGINT * INTERVAL '1 millisecond'),
                     heartbeat_at = clock_timestamp(),
                     updated_at = clock_timestamp()
                 WHERE (
                     existing.incarnation_id = EXCLUDED.incarnation_id
                     AND existing.status = 'active'
                 ) OR (
                     existing.incarnation_id <> EXCLUDED.incarnation_id
                     AND existing.lease_expires_at <= clock_timestamp()
                 )
                 RETURNING membership_epoch, status, lease_expires_at",
                &[&node_id.as_str(), &incarnation_id.as_str(), &lease_millis],
            )
            .await
            .map_err(|error| StorageError::Database(format!("join node membership: {error:?}")))?;

        let Some(row) = row else {
            return Err(StorageError::NodeLeaseBusy(node_id.as_str().to_owned()));
        };
        parse_node_membership(node_id.clone(), incarnation_id.clone(), &row)
    }

    /// Renews the current node lease using PostgreSQL time.
    ///
    /// A draining node may renew while it finishes existing work, but it cannot
    /// register new workers. A stale incarnation or expired lease is rejected.
    pub async fn heartbeat_node(
        &self,
        membership: &NodeMembership,
        lease_duration: Duration,
    ) -> Result<NodeMembership, StorageError> {
        let lease_millis = lease_millis(lease_duration)?;
        let epoch = sql_i64(membership.epoch)?;
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let row = transaction
            .query_opt(
                "UPDATE cluster_nodes
                 SET lease_expires_at =
                         clock_timestamp() + ($4::BIGINT * INTERVAL '1 millisecond'),
                     heartbeat_at = clock_timestamp(),
                     updated_at = clock_timestamp()
                 WHERE node_id = $1
                   AND incarnation_id = $2
                   AND membership_epoch = $3
                   AND status IN ('active', 'draining')
                   AND lease_expires_at > clock_timestamp()
                 RETURNING status, lease_expires_at",
                &[
                    &membership.node_id.as_str(),
                    &membership.incarnation_id.as_str(),
                    &epoch,
                    &lease_millis,
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let row = row.ok_or_else(|| {
            StorageError::NodeMembershipLost(membership.node_id.as_str().to_owned())
        })?;
        let status = MembershipStatus::parse(row.get(0))?;
        let lease_expires_at: SystemTime = row.get(1);

        // A node lease is the upper bound for every worker lease it owns. If
        // an operator or heartbeat shortens that parent lease, descendants are
        // capped in the same transaction using PostgreSQL-owned time.
        transaction
            .execute(
                "UPDATE worker_registrations
                 SET lease_expires_at = LEAST(lease_expires_at, $3::TIMESTAMPTZ),
                     updated_at = clock_timestamp()
                 WHERE node_id = $1
                   AND membership_epoch = $2
                   AND lease_expires_at > $3::TIMESTAMPTZ",
                &[&membership.node_id.as_str(), &epoch, &lease_expires_at],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        Ok(NodeMembership {
            node_id: membership.node_id.clone(),
            incarnation_id: membership.incarnation_id.clone(),
            epoch: membership.epoch,
            status,
            lease_expires_at,
        })
    }

    /// Marks a live node and all of its active workers as draining atomically.
    pub async fn drain_node(
        &self,
        membership: &NodeMembership,
    ) -> Result<NodeMembership, StorageError> {
        let epoch = sql_i64(membership.epoch)?;
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let row = transaction
            .query_opt(
                "UPDATE cluster_nodes
                 SET status = 'draining',
                     updated_at = clock_timestamp()
                 WHERE node_id = $1
                   AND incarnation_id = $2
                   AND membership_epoch = $3
                   AND status IN ('active', 'draining')
                   AND lease_expires_at > clock_timestamp()
                 RETURNING status, lease_expires_at",
                &[
                    &membership.node_id.as_str(),
                    &membership.incarnation_id.as_str(),
                    &epoch,
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let row = row.ok_or_else(|| {
            StorageError::NodeMembershipLost(membership.node_id.as_str().to_owned())
        })?;
        transaction
            .execute(
                "UPDATE worker_registrations
                 SET status = 'draining', updated_at = clock_timestamp()
                 WHERE node_id = $1 AND membership_epoch = $2 AND status = 'active'",
                &[&membership.node_id.as_str(), &epoch],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        Ok(NodeMembership {
            node_id: membership.node_id.clone(),
            incarnation_id: membership.incarnation_id.clone(),
            epoch: membership.epoch,
            status: MembershipStatus::parse(row.get(0))?,
            lease_expires_at: row.get(1),
        })
    }

    /// Registers a worker under a live, active node incarnation.
    ///
    /// The worker lease is capped at the node lease expiry. Worker IDs are
    /// process-instance identities and should not be reused across restarts.
    pub async fn register_worker(
        &self,
        membership: &NodeMembership,
        worker_id: &WorkerId,
        concurrency_limit: NonZeroU32,
        lease_duration: Duration,
    ) -> Result<WorkerRegistration, StorageError> {
        let lease_millis = lease_millis(lease_duration)?;
        let concurrency = i32::try_from(concurrency_limit.get())
            .map_err(|_| StorageError::InvalidWorkerCapacity)?;
        let epoch = sql_i64(membership.epoch)?;
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let node = transaction
            .query_opt(
                "SELECT status, lease_expires_at
                 FROM cluster_nodes
                 WHERE node_id = $1
                   AND incarnation_id = $2
                   AND membership_epoch = $3
                   AND lease_expires_at > clock_timestamp()
                 FOR UPDATE",
                &[
                    &membership.node_id.as_str(),
                    &membership.incarnation_id.as_str(),
                    &epoch,
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let Some(node) = node else {
            return Err(StorageError::NodeMembershipLost(
                membership.node_id.as_str().to_owned(),
            ));
        };
        let node_status: String = node.get(0);
        let node_lease_expiry: SystemTime = node.get(1);
        if node_status != "active" {
            return Err(StorageError::NodeMembershipLost(
                membership.node_id.as_str().to_owned(),
            ));
        }

        let row = transaction
            .query_opt(
                "INSERT INTO worker_registrations AS existing (
                     worker_id, node_id, membership_epoch, status,
                     concurrency_limit, lease_expires_at, heartbeat_at,
                     created_at, updated_at
                 )
                 SELECT $1, $2, $3, 'active', $4,
                        LEAST(
                            clock_timestamp() + ($6::BIGINT * INTERVAL '1 millisecond'),
                            $5::TIMESTAMPTZ
                        ),
                        clock_timestamp(), clock_timestamp(), clock_timestamp()
                 WHERE $5::TIMESTAMPTZ > clock_timestamp()
                 ON CONFLICT (worker_id) DO UPDATE
                 SET node_id = EXCLUDED.node_id,
                     membership_epoch = EXCLUDED.membership_epoch,
                     status = 'active',
                     concurrency_limit = EXCLUDED.concurrency_limit,
                     lease_expires_at = EXCLUDED.lease_expires_at,
                     heartbeat_at = clock_timestamp(),
                     updated_at = clock_timestamp()
                 WHERE (
                     existing.node_id = EXCLUDED.node_id
                     AND existing.membership_epoch = EXCLUDED.membership_epoch
                     AND existing.status = 'active'
                     AND existing.lease_expires_at > clock_timestamp()
                 ) OR existing.lease_expires_at <= clock_timestamp()
                 RETURNING status, concurrency_limit, lease_expires_at",
                &[
                    &worker_id.as_str(),
                    &membership.node_id.as_str(),
                    &epoch,
                    &concurrency,
                    &node_lease_expiry,
                    &lease_millis,
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let Some(row) = row else {
            return Err(StorageError::WorkerRegistrationBusy(
                worker_id.as_str().to_owned(),
            ));
        };
        let registration = WorkerRegistration {
            worker_id: worker_id.clone(),
            node: membership.clone(),
            concurrency_limit: NonZeroU32::new(u32::try_from(row.get::<_, i32>(1)).map_err(
                |_| {
                    StorageError::InvalidStoredValue(
                        "worker concurrency limit is outside the supported range".to_owned(),
                    )
                },
            )?)
            .ok_or_else(|| {
                StorageError::InvalidStoredValue(
                    "worker concurrency limit must be positive".to_owned(),
                )
            })?,
            status: MembershipStatus::parse(row.get(0))?,
            lease_expires_at: row.get(2),
        };
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        Ok(registration)
    }

    /// Renews a worker registration while its node incarnation remains live.
    pub async fn heartbeat_worker(
        &self,
        registration: &WorkerRegistration,
        lease_duration: Duration,
    ) -> Result<WorkerRegistration, StorageError> {
        let lease_millis = lease_millis(lease_duration)?;
        let epoch = sql_i64(registration.node.epoch)?;
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let node = transaction
            .query_opt(
                "SELECT status, lease_expires_at
                 FROM cluster_nodes
                 WHERE node_id = $1
                   AND incarnation_id = $2
                   AND membership_epoch = $3
                   AND status IN ('active', 'draining')
                   AND lease_expires_at > clock_timestamp()
                 FOR UPDATE",
                &[
                    &registration.node.node_id.as_str(),
                    &registration.node.incarnation_id.as_str(),
                    &epoch,
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let Some(node) = node else {
            return Err(StorageError::WorkerRegistrationLost(
                registration.worker_id.as_str().to_owned(),
            ));
        };
        let node_lease_expiry: SystemTime = node.get(1);
        let row = transaction
            .query_opt(
                "UPDATE worker_registrations
                 SET lease_expires_at = LEAST(
                         clock_timestamp() + ($4::BIGINT * INTERVAL '1 millisecond'),
                         $5::TIMESTAMPTZ
                     ),
                     heartbeat_at = clock_timestamp(),
                     updated_at = clock_timestamp()
                 WHERE worker_id = $1
                   AND node_id = $2
                   AND membership_epoch = $3
                   AND status IN ('active', 'draining')
                   AND lease_expires_at > clock_timestamp()
                   AND $5::TIMESTAMPTZ > clock_timestamp()
                 RETURNING status, concurrency_limit, lease_expires_at",
                &[
                    &registration.worker_id.as_str(),
                    &registration.node.node_id.as_str(),
                    &epoch,
                    &lease_millis,
                    &node_lease_expiry,
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let row = row.ok_or_else(|| {
            StorageError::WorkerRegistrationLost(registration.worker_id.as_str().to_owned())
        })?;
        let updated = worker_from_row(registration, &row)?;
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        Ok(updated)
    }

    /// Marks a live worker as draining so it receives no new task claims.
    pub async fn drain_worker(
        &self,
        registration: &WorkerRegistration,
    ) -> Result<WorkerRegistration, StorageError> {
        let epoch = sql_i64(registration.node.epoch)?;
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let row = transaction
            .query_opt(
                "UPDATE worker_registrations AS worker
                 SET status = 'draining', updated_at = clock_timestamp()
                 WHERE worker.worker_id = $1
                   AND worker.node_id = $2
                   AND worker.membership_epoch = $3
                   AND worker.status IN ('active', 'draining')
                   AND worker.lease_expires_at > clock_timestamp()
                   AND EXISTS (
                       SELECT 1 FROM cluster_nodes AS node
                       WHERE node.node_id = $2
                         AND node.incarnation_id = $4
                         AND node.membership_epoch = $3
                         AND node.status IN ('active', 'draining')
                         AND node.lease_expires_at > clock_timestamp()
                   )
                 RETURNING worker.status, worker.concurrency_limit, worker.lease_expires_at",
                &[
                    &registration.worker_id.as_str(),
                    &registration.node.node_id.as_str(),
                    &epoch,
                    &registration.node.incarnation_id.as_str(),
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let row = row.ok_or_else(|| {
            StorageError::WorkerRegistrationLost(registration.worker_id.as_str().to_owned())
        })?;
        let updated = worker_from_row(registration, &row)?;
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        Ok(updated)
    }

    /// Lists registrations that are active under a live, active node incarnation.
    pub async fn active_workers(&self) -> Result<Vec<WorkerRegistration>, StorageError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let rows = client
            .query(
                "SELECT worker.worker_id, worker.node_id, node.incarnation_id,
                        worker.membership_epoch, node.status, node.lease_expires_at,
                        worker.status, worker.concurrency_limit, worker.lease_expires_at
                 FROM worker_registrations AS worker
                 JOIN cluster_nodes AS node
                   ON node.node_id = worker.node_id
                  AND node.membership_epoch = worker.membership_epoch
                 WHERE worker.status = 'active'
                   AND worker.lease_expires_at > clock_timestamp()
                   AND node.status = 'active'
                   AND node.lease_expires_at > clock_timestamp()
                 ORDER BY worker.node_id, worker.worker_id",
                &[],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        rows.iter().map(parse_active_worker).collect()
    }
}

fn parse_node_membership(
    node_id: NodeId,
    incarnation_id: NodeIncarnationId,
    row: &tokio_postgres::Row,
) -> Result<NodeMembership, StorageError> {
    let epoch: i64 = row.get(0);
    Ok(NodeMembership {
        node_id,
        incarnation_id,
        epoch: u64::try_from(epoch).map_err(|_| {
            StorageError::InvalidStoredValue("node membership epoch is negative".to_owned())
        })?,
        status: MembershipStatus::parse(row.get(1))?,
        lease_expires_at: row.get(2),
    })
}

fn worker_from_row(
    registration: &WorkerRegistration,
    row: &tokio_postgres::Row,
) -> Result<WorkerRegistration, StorageError> {
    let concurrency = u32::try_from(row.get::<_, i32>(1)).map_err(|_| {
        StorageError::InvalidStoredValue(
            "worker concurrency limit is outside the supported range".to_owned(),
        )
    })?;
    Ok(WorkerRegistration {
        worker_id: registration.worker_id.clone(),
        node: registration.node.clone(),
        concurrency_limit: NonZeroU32::new(concurrency).ok_or_else(|| {
            StorageError::InvalidStoredValue("worker concurrency must be positive".to_owned())
        })?,
        status: MembershipStatus::parse(row.get(0))?,
        lease_expires_at: row.get(2),
    })
}

fn parse_active_worker(row: &tokio_postgres::Row) -> Result<WorkerRegistration, StorageError> {
    let node_id = NodeId::parse(row.get::<_, String>(1)).map_err(|error| {
        StorageError::InvalidStoredValue(format!("invalid node ID in PostgreSQL: {error}"))
    })?;
    let incarnation_id = NodeIncarnationId::parse(row.get::<_, String>(2)).map_err(|error| {
        StorageError::InvalidStoredValue(format!(
            "invalid node incarnation ID in PostgreSQL: {error}"
        ))
    })?;
    let epoch: i64 = row.get(3);
    let node = NodeMembership {
        node_id,
        incarnation_id,
        epoch: u64::try_from(epoch).map_err(|_| {
            StorageError::InvalidStoredValue("node membership epoch is negative".to_owned())
        })?,
        status: MembershipStatus::parse(row.get(4))?,
        lease_expires_at: row.get(5),
    };
    let concurrency = u32::try_from(row.get::<_, i32>(7)).map_err(|_| {
        StorageError::InvalidStoredValue(
            "worker concurrency limit is outside the supported range".to_owned(),
        )
    })?;
    Ok(WorkerRegistration {
        worker_id: WorkerId::parse(row.get::<_, String>(0)).map_err(|error| {
            StorageError::InvalidStoredValue(format!("invalid worker ID in PostgreSQL: {error}"))
        })?,
        node,
        concurrency_limit: NonZeroU32::new(concurrency).ok_or_else(|| {
            StorageError::InvalidStoredValue("worker concurrency must be positive".to_owned())
        })?,
        status: MembershipStatus::parse(row.get(6))?,
        lease_expires_at: row.get(8),
    })
}

fn lease_millis(duration: Duration) -> Result<i64, StorageError> {
    let millis = duration.as_millis();
    if millis == 0 || millis > MAX_LEASE_MILLIS {
        return Err(StorageError::InvalidLeaseDuration);
    }
    i64::try_from(millis).map_err(|_| StorageError::InvalidLeaseDuration)
}

fn sql_i64(value: u64) -> Result<i64, StorageError> {
    i64::try_from(value).map_err(|_| {
        StorageError::InvalidStoredValue(
            "node membership epoch is outside PostgreSQL BIGINT range".to_owned(),
        )
    })
}
