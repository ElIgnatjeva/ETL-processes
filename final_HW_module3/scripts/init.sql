CREATE TABLE IF NOT EXISTS user_sessions (
    session_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    pages_visited JSONB,
    device JSONB,
    actions JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS event_logs (
    event_id VARCHAR(50) PRIMARY KEY,
    timestamp TIMESTAMP,
    event_type VARCHAR(50),
    user_id VARCHAR(50),
    session_id VARCHAR(50),
    details JSONB,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS support_tickets (
    ticket_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50),
    status VARCHAR(20),
    issue_type VARCHAR(50),
    messages JSONB,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS user_recommendations (
    user_id VARCHAR(50) PRIMARY KEY,
    recommended_products JSONB,
    last_updated TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS moderation_queue (
    review_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50),
    product_id VARCHAR(50),
    review_text TEXT,
    rating INTEGER,
    moderation_status VARCHAR(20),
    flags JSONB,
    submitted_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS replication_control (
    collection_name VARCHAR(50) PRIMARY KEY,
    last_loaded_id VARCHAR(50),
    last_loaded_at TIMESTAMP,
    records_loaded INTEGER
);
CREATE SCHEMA IF NOT EXISTS marts;