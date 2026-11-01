//! Validated workflow definitions and deterministic dependency ordering.

use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};

use aetherflow_core::domain::{TaskId, TaskRunId, WorkflowId, WorkflowRunId, WorkflowStatus};

use crate::repository::TaskRun;

/// One task in a workflow definition and the tasks that must succeed first.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTask {
    task_id: TaskId,
    dependencies: Vec<TaskId>,
}

impl WorkflowTask {
    /// Creates a workflow task with explicit upstream task dependencies.
    #[must_use]
    pub fn new(task_id: TaskId, dependencies: Vec<TaskId>) -> Self {
        Self {
            task_id,
            dependencies,
        }
    }

    /// Returns the task definition identifier.
    #[must_use]
    pub fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    /// Returns the task definitions that must succeed before this task is activated.
    #[must_use]
    pub fn dependencies(&self) -> &[TaskId] {
        &self.dependencies
    }
}

/// A validated, acyclic workflow definition.
///
/// Tasks are retained by identifier and a deterministic topological ordering is
/// computed at construction time. A definition contains no task input/output,
/// persistence, clock, or execution state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowDefinition {
    workflow_id: WorkflowId,
    tasks: BTreeMap<String, WorkflowTask>,
    execution_order: Vec<TaskId>,
}

impl WorkflowDefinition {
    /// Validates task identifiers and dependencies, rejecting cycles and ambiguity.
    pub fn new(
        workflow_id: WorkflowId,
        tasks: Vec<WorkflowTask>,
    ) -> Result<Self, WorkflowDefinitionError> {
        if tasks.is_empty() {
            return Err(WorkflowDefinitionError::EmptyWorkflow);
        }

        let mut task_map = BTreeMap::new();
        for task in tasks {
            let key = task.task_id.as_str().to_owned();
            if let Entry::Vacant(entry) = task_map.entry(key.clone()) {
                entry.insert(task);
            } else {
                return Err(WorkflowDefinitionError::DuplicateTask(key));
            }
        }

        let mut indegree = BTreeMap::new();
        let mut dependents: BTreeMap<String, Vec<String>> = BTreeMap::new();

        for (task_key, task) in &task_map {
            let mut seen_dependencies = BTreeSet::new();

            for dependency in &task.dependencies {
                let dependency_key = dependency.as_str().to_owned();

                if dependency_key == *task_key {
                    return Err(WorkflowDefinitionError::SelfDependency(task_key.clone()));
                }

                if !seen_dependencies.insert(dependency_key.clone()) {
                    return Err(WorkflowDefinitionError::DuplicateDependency {
                        task_id: task_key.clone(),
                        dependency_id: dependency_key,
                    });
                }

                if !task_map.contains_key(&dependency_key) {
                    return Err(WorkflowDefinitionError::MissingDependency {
                        task_id: task_key.clone(),
                        dependency_id: dependency_key,
                    });
                }

                dependents
                    .entry(dependency_key)
                    .or_default()
                    .push(task_key.clone());
            }

            indegree.insert(task_key.clone(), task.dependencies.len());
        }

        for children in dependents.values_mut() {
            children.sort();
        }

        let mut ready = indegree
            .iter()
            .filter_map(|(task_id, count)| (*count == 0).then_some(task_id.clone()))
            .collect::<BTreeSet<_>>();
        let mut execution_order = Vec::with_capacity(task_map.len());

        while let Some(task_key) = ready.iter().next().cloned() {
            ready.remove(&task_key);
            execution_order.push(
                task_map
                    .get(&task_key)
                    .expect("ready task must exist")
                    .task_id
                    .clone(),
            );

            if let Some(children) = dependents.get(&task_key) {
                for child in children {
                    let count = indegree
                        .get_mut(child)
                        .expect("dependent task must have an indegree");
                    *count -= 1;
                    if *count == 0 {
                        ready.insert(child.clone());
                    }
                }
            }
        }

        if execution_order.len() != task_map.len() {
            let blocked_tasks = indegree
                .iter()
                .filter_map(|(task_id, count)| (*count > 0).then_some(task_id.clone()))
                .collect();
            return Err(WorkflowDefinitionError::Cycle(blocked_tasks));
        }

        Ok(Self {
            workflow_id,
            tasks: task_map,
            execution_order,
        })
    }

    /// Returns the workflow definition identifier.
    #[must_use]
    pub fn workflow_id(&self) -> &WorkflowId {
        &self.workflow_id
    }

    /// Returns the number of task definitions in the workflow.
    #[must_use]
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    /// Returns a deterministic topological ordering of task identifiers.
    #[must_use]
    pub fn execution_order(&self) -> &[TaskId] {
        &self.execution_order
    }

    /// Looks up one task definition.
    #[must_use]
    pub fn task(&self, task_id: &TaskId) -> Option<&WorkflowTask> {
        self.tasks.get(task_id.as_str())
    }

    /// Iterates over task definitions in identifier order.
    pub fn tasks(&self) -> impl Iterator<Item = &WorkflowTask> {
        self.tasks.values()
    }
}

/// Validation failures for workflow definitions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkflowDefinitionError {
    /// A workflow must contain at least one task.
    EmptyWorkflow,
    /// A task identifier occurs more than once in one workflow.
    DuplicateTask(String),
    /// A task directly depends on itself.
    SelfDependency(String),
    /// A dependency is declared more than once for one task.
    DuplicateDependency {
        /// The task declaring the duplicate.
        task_id: String,
        /// The duplicated dependency.
        dependency_id: String,
    },
    /// A dependency does not belong to this workflow.
    MissingDependency {
        /// The task with the unresolved dependency.
        task_id: String,
        /// The unresolved dependency.
        dependency_id: String,
    },
    /// At least one cycle prevents a topological ordering.
    Cycle(Vec<String>),
}

impl std::fmt::Display for WorkflowDefinitionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyWorkflow => formatter.write_str("workflow must contain at least one task"),
            Self::DuplicateTask(task_id) => {
                write!(formatter, "duplicate workflow task: {task_id}")
            }
            Self::SelfDependency(task_id) => {
                write!(formatter, "task cannot depend on itself: {task_id}")
            }
            Self::DuplicateDependency {
                task_id,
                dependency_id,
            } => write!(
                formatter,
                "task {task_id} declares dependency {dependency_id} more than once"
            ),
            Self::MissingDependency {
                task_id,
                dependency_id,
            } => write!(
                formatter,
                "task {task_id} depends on unknown task {dependency_id}"
            ),
            Self::Cycle(task_ids) => {
                write!(
                    formatter,
                    "workflow contains a dependency cycle: {task_ids:?}"
                )
            }
        }
    }
}

impl std::error::Error for WorkflowDefinitionError {}

#[cfg(test)]
mod tests {
    use super::{WorkflowDefinition, WorkflowDefinitionError, WorkflowTask};
    use aetherflow_core::domain::{TaskId, WorkflowId};

    fn task(id: &str, dependencies: &[&str]) -> WorkflowTask {
        WorkflowTask::new(
            TaskId::parse(id).expect("valid task ID"),
            dependencies
                .iter()
                .map(|dependency| TaskId::parse(*dependency).expect("valid dependency ID"))
                .collect(),
        )
    }

    fn workflow_id() -> WorkflowId {
        WorkflowId::parse("workflow-01").expect("valid workflow ID")
    }


    #[test]
    fn sequence_generator_returns_unique_valid_task_run_ids() {
        let generator = super::SequenceTaskRunIdGenerator::new();
        let first = super::TaskRunIdGenerator::next_id(&generator).expect("first ID");
        let second = super::TaskRunIdGenerator::next_id(&generator).expect("second ID");

        assert_eq!(first.as_str(), "task-run-1");
        assert_eq!(second.as_str(), "task-run-2");
        assert_ne!(first, second);
    }

    #[test]
    fn computes_deterministic_topological_order() {
        let definition = WorkflowDefinition::new(
            workflow_id(),
            vec![
                task("task-c", &["task-b"]),
                task("task-d", &[]),
                task("task-b", &["task-a"]),
                task("task-a", &[]),
            ],
        )
        .expect("valid DAG");

        let ordered = definition
            .execution_order()
            .iter()
            .map(TaskId::as_str)
            .collect::<Vec<_>>();

        assert_eq!(ordered, ["task-a", "task-b", "task-c", "task-d"]);
    }

    #[test]
    fn rejects_empty_workflow() {
        assert_eq!(
            WorkflowDefinition::new(workflow_id(), Vec::new()),
            Err(WorkflowDefinitionError::EmptyWorkflow)
        );
    }

    #[test]
    fn rejects_duplicate_task_ids() {
        assert_eq!(
            WorkflowDefinition::new(
                workflow_id(),
                vec![task("task-a", &[]), task("task-a", &[])],
            ),
            Err(WorkflowDefinitionError::DuplicateTask("task-a".to_owned()))
        );
    }

    #[test]
    fn rejects_missing_dependencies() {
        assert_eq!(
            WorkflowDefinition::new(workflow_id(), vec![task("task-a", &["task-missing"])]),
            Err(WorkflowDefinitionError::MissingDependency {
                task_id: "task-a".to_owned(),
                dependency_id: "task-missing".to_owned(),
            })
        );
    }

    #[test]
    fn rejects_self_dependencies() {
        assert_eq!(
            WorkflowDefinition::new(workflow_id(), vec![task("task-a", &["task-a"])]),
            Err(WorkflowDefinitionError::SelfDependency("task-a".to_owned()))
        );
    }

    #[test]
    fn rejects_duplicate_dependencies() {
        assert_eq!(
            WorkflowDefinition::new(
                workflow_id(),
                vec![task("task-a", &[]), task("task-b", &["task-a", "task-a"]),],
            ),
            Err(WorkflowDefinitionError::DuplicateDependency {
                task_id: "task-b".to_owned(),
                dependency_id: "task-a".to_owned(),
            })
        );
    }

    #[test]
    fn rejects_cycles() {
        assert_eq!(
            WorkflowDefinition::new(
                workflow_id(),
                vec![task("task-a", &["task-b"]), task("task-b", &["task-a"])],
            ),
            Err(WorkflowDefinitionError::Cycle(vec![
                "task-a".to_owned(),
                "task-b".to_owned(),
            ]))
        );
    }
}


/// Generates task-run identifiers when a workflow execution is materialized.
pub trait TaskRunIdGenerator: Send + Sync {
    /// Returns a unique task-run identifier within the generator's namespace.
    fn next_id(&self) -> Result<TaskRunId, TaskRunIdGenerationError>;
}

/// A process-local monotonically increasing task-run ID generator.
///
/// Reuse one generator across all workflow runs sharing a repository. This
/// sequence is intentionally not durable; it is appropriate only for the
/// process-local in-memory runtime.
#[derive(Debug)]
pub struct SequenceTaskRunIdGenerator {
    next: std::sync::atomic::AtomicU64,
}

impl SequenceTaskRunIdGenerator {
    /// Creates a generator whose first identifier ends in sequence number one.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            next: std::sync::atomic::AtomicU64::new(1),
        }
    }
}

impl Default for SequenceTaskRunIdGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskRunIdGenerator for SequenceTaskRunIdGenerator {
    fn next_id(&self) -> Result<TaskRunId, TaskRunIdGenerationError> {
        use std::sync::atomic::Ordering;

        let sequence = self
            .next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| TaskRunIdGenerationError::SequenceExhausted)?;

        TaskRunId::parse(format!("task-run-{sequence}"))
            .map_err(|error| TaskRunIdGenerationError::InvalidIdentifier(error.to_string()))
    }
}

/// Failures returned by task-run ID generators.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskRunIdGenerationError {
    /// The generator exhausted its u64 sequence space.
    SequenceExhausted,
    /// The generated value does not satisfy the domain ID contract.
    InvalidIdentifier(String),
}

impl std::fmt::Display for TaskRunIdGenerationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SequenceExhausted => formatter.write_str("task-run ID sequence exhausted"),
            Self::InvalidIdentifier(message) => {
                write!(formatter, "generated task-run identifier is invalid: {message}")
            }
        }
    }
}

impl std::error::Error for TaskRunIdGenerationError {}

/// Final snapshot returned after a workflow execution reaches a terminal state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowExecutionResult {
    workflow_id: WorkflowId,
    workflow_run_id: WorkflowRunId,
    status: WorkflowStatus,
    version: u64,
    task_runs: Vec<TaskRun>,
}

impl WorkflowExecutionResult {
    pub(crate) fn new(
        workflow_id: WorkflowId,
        workflow_run_id: WorkflowRunId,
        status: WorkflowStatus,
        version: u64,
        task_runs: Vec<TaskRun>,
    ) -> Self {
        Self {
            workflow_id,
            workflow_run_id,
            status,
            version,
            task_runs,
        }
    }

    /// Returns the workflow definition identifier.
    #[must_use]
    pub fn workflow_id(&self) -> &WorkflowId {
        &self.workflow_id
    }

    /// Returns this workflow execution identifier.
    #[must_use]
    pub fn workflow_run_id(&self) -> &WorkflowRunId {
        &self.workflow_run_id
    }

    /// Returns the terminal workflow lifecycle state.
    #[must_use]
    pub const fn status(&self) -> WorkflowStatus {
        self.status
    }

    /// Returns the workflow-run optimistic concurrency version.
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Returns snapshots of task runs materialized during this workflow execution.
    ///
    /// Tasks whose dependencies never succeeded are not materialized and are
    /// therefore absent from this collection.
    #[must_use]
    pub fn task_runs(&self) -> &[TaskRun] {
        &self.task_runs
    }
}

/// Errors raised while coordinating a workflow run.
#[derive(Debug)]
pub enum WorkflowExecutionError {
    /// The supplied workflow definition is invalid.
    InvalidDefinition(WorkflowDefinitionError),
    /// Task-run identifier generation failed.
    IdGeneration(TaskRunIdGenerationError),
    /// A domain lifecycle transition was rejected.
    InvalidTransition(aetherflow_core::domain::TransitionError),
    /// The task repository or scheduler failed.
    Scheduler(crate::scheduler::SchedulerError),
    /// The worker loop exited before the workflow reached a terminal state.
    WorkerLoopExited,
    /// The worker loop task failed to join.
    WorkerJoin(String),
}

impl std::fmt::Display for WorkflowExecutionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidDefinition(error) => error.fmt(formatter),
            Self::IdGeneration(error) => error.fmt(formatter),
            Self::InvalidTransition(error) => error.fmt(formatter),
            Self::Scheduler(error) => error.fmt(formatter),
            Self::WorkerLoopExited => {
                formatter.write_str("worker loop exited before workflow completion")
            }
            Self::WorkerJoin(message) => write!(formatter, "worker loop join failed: {message}"),
        }
    }
}

impl std::error::Error for WorkflowExecutionError {}

impl From<WorkflowDefinitionError> for WorkflowExecutionError {
    fn from(value: WorkflowDefinitionError) -> Self {
        Self::InvalidDefinition(value)
    }
}

impl From<TaskRunIdGenerationError> for WorkflowExecutionError {
    fn from(value: TaskRunIdGenerationError) -> Self {
        Self::IdGeneration(value)
    }
}

impl From<aetherflow_core::domain::TransitionError> for WorkflowExecutionError {
    fn from(value: aetherflow_core::domain::TransitionError) -> Self {
        Self::InvalidTransition(value)
    }
}


impl From<crate::repository::RepositoryError> for WorkflowExecutionError {
    fn from(value: crate::repository::RepositoryError) -> Self {
        Self::Scheduler(crate::scheduler::SchedulerError::Repository(value))
    }
}

impl From<crate::scheduler::SchedulerError> for WorkflowExecutionError {
    fn from(value: crate::scheduler::SchedulerError) -> Self {
        Self::Scheduler(value)
    }
}
