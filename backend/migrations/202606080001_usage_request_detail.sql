-- +goose Up
DROP TABLE IF EXISTS user_quota_charges;
DROP TABLE IF EXISTS usage_records;

CREATE TABLE usage_records (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	created_at DATETIME NOT NULL,
	timestamp DATETIME NOT NULL,
	usage_username VARCHAR(120),
	api_key_description VARCHAR(240),
	provider VARCHAR(120),
	executor_type VARCHAR(120),
	model VARCHAR(180),
	alias VARCHAR(180),
	reasoning_effort VARCHAR(80),
	service_tier VARCHAR(80),
	endpoint VARCHAR(240),
	inbound_endpoint VARCHAR(240),
	upstream_endpoint VARCHAR(240),
	upstream_transport VARCHAR(40),
	upstream_url VARCHAR(500),
	source VARCHAR(120),
	source_account VARCHAR(320),
	request_id VARCHAR(240),
	auth VARCHAR(120),
	auth_id VARCHAR(240),
	auth_index VARCHAR(500),
	status_code INTEGER,
	latency_ms REAL,
	ttft_ms REAL,
	failed BOOLEAN NOT NULL DEFAULT 0,
	input_tokens INTEGER NOT NULL DEFAULT 0,
	output_tokens INTEGER NOT NULL DEFAULT 0,
	cached_tokens INTEGER NOT NULL DEFAULT 0,
	cache_read_tokens INTEGER NOT NULL DEFAULT 0,
	cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
	reasoning_tokens INTEGER NOT NULL DEFAULT 0,
	total_tokens INTEGER NOT NULL DEFAULT 0,
	dedupe_key VARCHAR(80) NOT NULL UNIQUE,
	raw_json TEXT NOT NULL
);

CREATE TABLE user_quota_charges (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	usage_record_id INTEGER NOT NULL UNIQUE,
	user_id INTEGER NOT NULL,
	usage_username VARCHAR(120) NOT NULL,
	amount_usd REAL NOT NULL DEFAULT 0,
	monthly_deducted_usd REAL NOT NULL DEFAULT 0,
	lifetime_deducted_usd REAL NOT NULL DEFAULT 0,
	unpriced BOOLEAN NOT NULL DEFAULT 0,
	quota_month VARCHAR(7) NOT NULL,
	created_at DATETIME NOT NULL,
	FOREIGN KEY(usage_record_id) REFERENCES usage_records(id),
	FOREIGN KEY(user_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS ix_usage_records_timestamp ON usage_records(timestamp);
CREATE INDEX IF NOT EXISTS ix_usage_records_usage_username ON usage_records(usage_username);
CREATE INDEX IF NOT EXISTS ix_usage_records_provider ON usage_records(provider);
CREATE INDEX IF NOT EXISTS ix_usage_records_model ON usage_records(model);
CREATE INDEX IF NOT EXISTS ix_usage_records_executor_type ON usage_records(executor_type);
CREATE INDEX IF NOT EXISTS ix_usage_records_endpoint ON usage_records(endpoint);
CREATE INDEX IF NOT EXISTS ix_usage_records_inbound_endpoint ON usage_records(inbound_endpoint);
CREATE INDEX IF NOT EXISTS ix_usage_records_upstream_endpoint ON usage_records(upstream_endpoint);
CREATE INDEX IF NOT EXISTS ix_usage_records_upstream_transport ON usage_records(upstream_transport);
CREATE INDEX IF NOT EXISTS ix_usage_records_failed ON usage_records(failed);
CREATE INDEX IF NOT EXISTS ix_usage_records_source_account_timestamp ON usage_records(source_account, timestamp);
CREATE INDEX IF NOT EXISTS ix_usage_records_auth_index_timestamp ON usage_records(auth_index, timestamp);
CREATE INDEX IF NOT EXISTS ix_user_quota_charges_user_id ON user_quota_charges(user_id);
CREATE INDEX IF NOT EXISTS ix_user_quota_charges_created_at ON user_quota_charges(created_at);
CREATE INDEX IF NOT EXISTS ix_user_quota_charges_quota_month ON user_quota_charges(quota_month);

-- +goose Down
DROP TABLE IF EXISTS user_quota_charges;
DROP TABLE IF EXISTS usage_records;
