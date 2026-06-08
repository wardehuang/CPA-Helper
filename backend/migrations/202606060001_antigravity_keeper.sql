-- +goose Up
ALTER TABLE app_settings ADD COLUMN antigravity_keeper_settings TEXT NOT NULL DEFAULT '{}';
ALTER TABLE app_settings ADD COLUMN antigravity_keeper_priority_rules TEXT NOT NULL DEFAULT '{}';

CREATE TABLE IF NOT EXISTS antigravity_keeper_auth_states (
	auth_name VARCHAR(500) PRIMARY KEY,
	email VARCHAR(320),
	account_type VARCHAR(80),
	disabled BOOLEAN NOT NULL DEFAULT 0,
	priority INTEGER,
	restore_priority INTEGER,
	latest_action TEXT,
	last_error TEXT,
	last_status_code INTEGER,
	primary_used_percent INTEGER,
	secondary_used_percent INTEGER,
	primary_reset_at DATETIME,
	secondary_reset_at DATETIME,
	primary_window_seconds INTEGER,
	secondary_window_seconds INTEGER,
	quota_threshold INTEGER,
	last_checked_at DATETIME,
	last_healthy_at DATETIME,
	created_at DATETIME NOT NULL,
	updated_at DATETIME NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_antigravity_keeper_auth_states_last_checked_at ON antigravity_keeper_auth_states(last_checked_at);

CREATE TABLE IF NOT EXISTS antigravity_keeper_runs (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	mode VARCHAR(20) NOT NULL,
	state VARCHAR(20) NOT NULL,
	detail TEXT,
	started_at DATETIME NOT NULL,
	finished_at DATETIME,
	total INTEGER NOT NULL DEFAULT 0,
	healthy INTEGER NOT NULL DEFAULT 0,
	status_disabled INTEGER NOT NULL DEFAULT 0,
	status_enabled INTEGER NOT NULL DEFAULT 0,
	priority_degraded INTEGER NOT NULL DEFAULT 0,
	priority_restored INTEGER NOT NULL DEFAULT 0,
	skipped INTEGER NOT NULL DEFAULT 0,
	network_error INTEGER NOT NULL DEFAULT 0,
	created_at DATETIME NOT NULL,
	updated_at DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS antigravity_keeper_run_accounts (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	run_id INTEGER NOT NULL,
	auth_name VARCHAR(500) NOT NULL,
	email VARCHAR(320),
	result VARCHAR(40) NOT NULL,
	account_type VARCHAR(80),
	priority INTEGER,
	disabled BOOLEAN,
	keeper_action VARCHAR(40) NOT NULL DEFAULT 'none',
	primary_used_percent INTEGER,
	secondary_used_percent INTEGER,
	quota_threshold INTEGER,
	last_status_code INTEGER,
	last_error TEXT,
	latest_action TEXT,
	checked_at DATETIME NOT NULL,
	created_at DATETIME NOT NULL,
	FOREIGN KEY(run_id) REFERENCES antigravity_keeper_runs(id)
);

-- +goose Down
DROP TABLE IF EXISTS antigravity_keeper_run_accounts;
DROP TABLE IF EXISTS antigravity_keeper_runs;
DROP TABLE IF EXISTS antigravity_keeper_auth_states;
