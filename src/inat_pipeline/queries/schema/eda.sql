CREATE SCHEMA IF NOT EXISTS eda;

CREATE TABLE IF NOT EXISTS eda.splits (
    month DATE,
    bucket VARCHAR,
    n_obs UBIGINT,
    pos_rate DOUBLE
);
