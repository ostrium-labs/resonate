-- Retention's schema (FW5). Not a migration: it is applied when the engine is
-- opened with `retention` configured, and only then, so a deployment that
-- never turns retention on has nothing new to migrate to. Every statement is
-- idempotent.

-- A pruned id, reserved until `expires_at`. While the row exists, every
-- request that names the id answers 410 `expired` instead of acting on it or
-- creating it again.
CREATE TABLE IF NOT EXISTS resonate.promise_tombstones (
  id         TEXT   PRIMARY KEY,
  pruned_at  BIGINT NOT NULL,
  expires_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_promise_tombstones_expires_at
  ON resonate.promise_tombstones (expires_at);

-- How a pass finds what to prune without reading the table: settled rows by
-- settlement, and internal promises past their deadline (settled in all but
-- the row; nothing arms a timer for them) by deadline.
CREATE INDEX IF NOT EXISTS idx_promises_settled_at
  ON resonate.promises (settled_at) WHERE state <> 'pending';
CREATE INDEX IF NOT EXISTS idx_promises_lapsed_at
  ON resonate.promises (timeout_at) WHERE state = 'pending' AND NOT external;
