DROP TABLE IF EXISTS stg_fund_db.audit_table;
CREATE TABLE stg_fund_db.audit_table
(
    start_load_date TIMESTAMP,
    end_load_date   TIMESTAMP,
    task            VARCHAR(256),
    is_stored       BOOLEAN
);