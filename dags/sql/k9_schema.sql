-- create pet table
CREATE TABLE IF NOT EXISTS k9_facts (
    fact_id SERIAL PRIMARY KEY,
    created_date TIMESTAMP NOT NULL,
    description TEXT NOT NULL);