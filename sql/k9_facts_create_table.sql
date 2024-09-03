-- create k9_facts table
CREATE TABLE IF NOT EXISTS k9_facts (
    id SERIAL PRIMARY KEY,
    fact_id TEXT UNIQUE,
    created_date TIMESTAMP,
    description TEXT,
    category VARCHAR(50),
    last_modified_date TIMESTAMP,
    is_deleted BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_k9_facts_fact_id ON k9_facts(fact_id);
CREATE INDEX IF NOT EXISTS idx_k9_facts_last_modified ON k9_facts(last_modified_date);