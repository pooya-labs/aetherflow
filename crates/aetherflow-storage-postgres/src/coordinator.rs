//! Durable workflow materialization over the PostgreSQL task repository.

use std::collections::BTreeMap;
use std::time::SystemTime;

use aetherflow_core::domain::{IdempotencyKey, RetryPolicy, TaskId, TaskRunId, WorkflowRunId};
use aetherflow_runtime::workflow::WorkflowDefinition;
use sha2::{Digest, Sha256};

use crate::{NewTaskRun, PgTaskRepository, StorageError, TenantContext};

/// Identifiers assigned when a workflow definition is materialized durably.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PgWorkflowSubmission {
    workflow_run_id: WorkflowRunId,
    task_run_ids: BTreeMap<String, TaskRunId>,
}

impl PgWorkflowSubmission {
    /// Returns the durable workflow-run identifier.
    #[must_use]
    pub fn workflow_run_id(&self) -> &WorkflowRunId {
        &self.workflow_run_id
    }

    /// Returns the task-run identifier assigned to one task definition.
    #[must_use]
    pub fn task_run_id(&self, task_id: &TaskId) -> Option<&TaskRunId> {
        self.task_run_ids.get(task_id.as_str())
    }

    /// Iterates over task definitions and their durable task-run identifiers.
    pub fn task_runs(&self) -> impl Iterator<Item = (&str, &TaskRunId)> {
        self.task_run_ids
            .iter()
            .map(|(task_id, task_run_id)| (task_id.as_str(), task_run_id))
    }
}

/// Materializes validated runtime workflow definitions into PostgreSQL.
#[derive(Clone)]
pub struct PgWorkflowCoordinator {
    repository: std::sync::Arc<PgTaskRepository>,
}

impl PgWorkflowCoordinator {
    /// Creates a workflow coordinator over a shared PostgreSQL repository.
    #[must_use]
    pub fn new(repository: std::sync::Arc<PgTaskRepository>) -> Self {
        Self { repository }
    }

    /// Materializes all task runs with deterministic identifiers and database-time
    /// dependency gating. Task-run IDs are derived from the workflow-run and task
    /// definition IDs, so replaying the same submission does not depend on a
    /// process-local sequence generator.
    pub async fn submit_workflow(
        &self,
        workflow_run_id: &WorkflowRunId,
        definition: &WorkflowDefinition,
        retry_policy: RetryPolicy,
        idempotency_key: Option<&IdempotencyKey>,
    ) -> Result<PgWorkflowSubmission, StorageError> {
        self.submit_workflow_at(
            workflow_run_id,
            definition,
            retry_policy,
            idempotency_key,
            SystemTime::now(),
        )
        .await
    }

    /// Materializes a workflow with an explicit absolute availability timestamp.
    ///
    /// The timestamp is persisted as PostgreSQL `TIMESTAMPTZ`; it is not derived
    /// from the runtime's process-local monotonic clock.
    pub async fn submit_workflow_at(
        &self,
        workflow_run_id: &WorkflowRunId,
        definition: &WorkflowDefinition,
        retry_policy: RetryPolicy,
        idempotency_key: Option<&IdempotencyKey>,
        available_at: SystemTime,
    ) -> Result<PgWorkflowSubmission, StorageError> {
        self.submit_workflow_inner(
            None,
            workflow_run_id,
            definition,
            retry_policy,
            idempotency_key,
            available_at,
        )
        .await
    }

    /// Materializes a workflow for the authenticated tenant using the current time.
    pub async fn submit_workflow_for_tenant(
        &self,
        context: &TenantContext,
        workflow_run_id: &WorkflowRunId,
        definition: &WorkflowDefinition,
        retry_policy: RetryPolicy,
        idempotency_key: Option<&IdempotencyKey>,
    ) -> Result<PgWorkflowSubmission, StorageError> {
        self.submit_workflow_for_tenant_at(
            context,
            workflow_run_id,
            definition,
            retry_policy,
            idempotency_key,
            SystemTime::now(),
        )
        .await
    }

    /// Materializes a tenant-owned workflow at an explicit absolute timestamp.
    pub async fn submit_workflow_for_tenant_at(
        &self,
        context: &TenantContext,
        workflow_run_id: &WorkflowRunId,
        definition: &WorkflowDefinition,
        retry_policy: RetryPolicy,
        idempotency_key: Option<&IdempotencyKey>,
        available_at: SystemTime,
    ) -> Result<PgWorkflowSubmission, StorageError> {
        self.submit_workflow_inner(
            Some(context),
            workflow_run_id,
            definition,
            retry_policy,
            idempotency_key,
            available_at,
        )
        .await
    }

    async fn submit_workflow_inner(
        &self,
        context: Option<&TenantContext>,
        workflow_run_id: &WorkflowRunId,
        definition: &WorkflowDefinition,
        retry_policy: RetryPolicy,
        idempotency_key: Option<&IdempotencyKey>,
        available_at: SystemTime,
    ) -> Result<PgWorkflowSubmission, StorageError> {
        let mut task_run_ids = BTreeMap::new();
        for task in definition.tasks() {
            task_run_ids.insert(
                task.task_id().as_str().to_owned(),
                deterministic_task_run_id(workflow_run_id, task.task_id())?,
            );
        }

        let mut durable_tasks = Vec::with_capacity(definition.task_count());
        for task in definition.tasks() {
            let task_run_id = task_run_ids
                .get(task.task_id().as_str())
                .expect("every validated task has a deterministic run ID")
                .clone();
            let dependencies = task
                .dependencies()
                .iter()
                .map(|dependency| {
                    task_run_ids
                        .get(dependency.as_str())
                        .expect("validated dependencies have task-run IDs")
                        .clone()
                })
                .collect();
            durable_tasks.push(NewTaskRun::new(
                task_run_id,
                task.task_id().clone(),
                task.input().clone(),
                dependencies,
                retry_policy,
                available_at,
            ));
        }

        if let Some(context) = context {
            self.repository
                .create_workflow_for_tenant(
                    context,
                    workflow_run_id,
                    definition.workflow_id(),
                    idempotency_key,
                    &durable_tasks,
                )
                .await?;
        } else {
            self.repository
                .create_workflow(
                    workflow_run_id,
                    definition.workflow_id(),
                    idempotency_key,
                    &durable_tasks,
                )
                .await?;
        }

        Ok(PgWorkflowSubmission {
            workflow_run_id: workflow_run_id.clone(),
            task_run_ids,
        })
    }
}

fn deterministic_task_run_id(
    workflow_run_id: &WorkflowRunId,
    task_id: &TaskId,
) -> Result<TaskRunId, StorageError> {
    let mut digest = Sha256::new();
    digest.update(workflow_run_id.as_str().as_bytes());
    digest.update([0]);
    digest.update(task_id.as_str().as_bytes());
    let bytes = digest.finalize();
    let suffix = bytes[..20]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    TaskRunId::parse(format!("tr-{suffix}"))
        .map_err(|error| StorageError::InvalidStoredValue(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::deterministic_task_run_id;
    use aetherflow_core::domain::{TaskId, WorkflowRunId};

    #[test]
    fn durable_task_run_ids_are_stable_and_workflow_scoped() {
        let workflow_a = WorkflowRunId::parse("workflow-run-a").expect("valid workflow ID");
        let workflow_b = WorkflowRunId::parse("workflow-run-b").expect("valid workflow ID");
        let task = TaskId::parse("charge-card").expect("valid task ID");

        let first = deterministic_task_run_id(&workflow_a, &task).expect("task-run ID");
        let replay = deterministic_task_run_id(&workflow_a, &task).expect("replayed task-run ID");
        let other_workflow =
            deterministic_task_run_id(&workflow_b, &task).expect("other workflow task-run ID");

        assert_eq!(first, replay);
        assert_ne!(first, other_workflow);
    }
}
