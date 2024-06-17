create database BICO;
grant all privileges on database "BICO" to postgres;

-- Connect to the BICO database
\c BICO;

-- Create the schema and tables
CREATE SCHEMA IF NOT EXISTS pipeline;

CREATE TABLE IF NOT EXISTS pipeline.bundlers (
    entity_name VARCHAR(255),
    address VARCHAR(255),
    ethereum BOOLEAN,
    polygon BOOLEAN,
    arbitrum BOOLEAN,
    optimism BOOLEAN,
    bnb BOOLEAN,
    avalanche BOOLEAN,
    base BOOLEAN
);

-- Load data from the CSV file into the bundlers table
COPY pipeline.bundlers(entity_name, address, ethereum, polygon, arbitrum, optimism, bnb, avalanche, base)
FROM '/docker-entrypoint-initdb.d/bundlers.csv' DELIMITER ',' CSV HEADER;