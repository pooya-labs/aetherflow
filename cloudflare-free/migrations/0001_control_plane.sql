CREATE TABLE IF NOT EXISTS demo_visitors (
  browser_hash TEXT PRIMARY KEY CHECK(length(browser_hash) = 64),
  workspace_id TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS demo_sessions (
  token_hash TEXT PRIMARY KEY CHECK(length(token_hash) = 64),
  workspace_id TEXT NOT NULL,
  expires_at INTEGER NOT NULL,
  read_count INTEGER NOT NULL DEFAULT 0 CHECK(read_count >= 0),
  mutation_count INTEGER NOT NULL DEFAULT 0 CHECK(mutation_count >= 0),
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS demo_sessions_expiry_idx ON demo_sessions(expires_at);
CREATE INDEX IF NOT EXISTS demo_sessions_workspace_idx ON demo_sessions(workspace_id);

CREATE TABLE IF NOT EXISTS demo_daily_usage (
  usage_day TEXT NOT NULL,
  kind TEXT NOT NULL,
  count INTEGER NOT NULL DEFAULT 0 CHECK(count >= 0),
  PRIMARY KEY(usage_day, kind)
);

CREATE TABLE IF NOT EXISTS demo_workflows (
  workspace_id TEXT NOT NULL,
  workflow_run_id TEXT NOT NULL,
  workflow_id TEXT NOT NULL,
  workflow_instance_id TEXT UNIQUE,
  status TEXT NOT NULL CHECK(status IN ('pending','running','succeeded','failed','cancelled','timed_out')),
  version INTEGER NOT NULL DEFAULT 1 CHECK(version > 0),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  finished_at TEXT,
  PRIMARY KEY(workspace_id, workflow_run_id),
  FOREIGN KEY(workspace_id) REFERENCES demo_visitors(workspace_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS demo_workflows_list_idx ON demo_workflows(workspace_id, created_at DESC, workflow_run_id DESC);
CREATE INDEX IF NOT EXISTS demo_workflows_status_idx ON demo_workflows(workspace_id, status, created_at DESC);

CREATE TABLE IF NOT EXISTS demo_tasks (
  workspace_id TEXT NOT NULL,
  workflow_run_id TEXT NOT NULL,
  task_run_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('pending','ready','running','retry_waiting','succeeded','failed','cancelled','timed_out')),
  version INTEGER NOT NULL DEFAULT 1 CHECK(version > 0),
  attempts_started INTEGER NOT NULL DEFAULT 0 CHECK(attempts_started >= 0),
  max_attempts INTEGER NOT NULL DEFAULT 1 CHECK(max_attempts > 0),
  initial_backoff_ms INTEGER NOT NULL DEFAULT 0 CHECK(initial_backoff_ms >= 0),
  max_backoff_ms INTEGER NOT NULL DEFAULT 0 CHECK(max_backoff_ms >= initial_backoff_ms),
  available_at TEXT NOT NULL,
  input_base64 TEXT NOT NULL DEFAULT '',
  input_size_bytes INTEGER NOT NULL DEFAULT 0 CHECK(input_size_bytes >= 0 AND input_size_bytes <= 8192),
  output_size_bytes INTEGER,
  output_present INTEGER NOT NULL DEFAULT 0 CHECK(output_present IN (0,1)),
  dependency_task_run_ids_json TEXT NOT NULL DEFAULT '[]',
  claimed_by TEXT,
  lease_expires_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  last_error_kind TEXT,
  last_error_message TEXT,
  PRIMARY KEY(workspace_id, task_run_id),
  UNIQUE(workspace_id, workflow_run_id, task_id),
  FOREIGN KEY(workspace_id, workflow_run_id) REFERENCES demo_workflows(workspace_id, workflow_run_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS demo_tasks_workflow_idx ON demo_tasks(workspace_id, workflow_run_id, task_id);
CREATE INDEX IF NOT EXISTS demo_tasks_status_idx ON demo_tasks(workspace_id, status, updated_at);

CREATE TABLE IF NOT EXISTS demo_events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  workspace_id TEXT NOT NULL,
  workflow_run_id TEXT NOT NULL,
  aggregate_type TEXT NOT NULL CHECK(aggregate_type IN ('workflow_run','task_run')),
  aggregate_id TEXT NOT NULL,
  aggregate_version INTEGER NOT NULL CHECK(aggregate_version > 0),
  event_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'recorded',
  created_at TEXT NOT NULL,
  published_at TEXT,
  UNIQUE(workspace_id, aggregate_type, aggregate_id, aggregate_version),
  FOREIGN KEY(workspace_id, workflow_run_id) REFERENCES demo_workflows(workspace_id, workflow_run_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS demo_events_workflow_idx ON demo_events(workspace_id, workflow_run_id, event_id);
