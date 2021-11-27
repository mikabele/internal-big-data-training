USE stg_fund_db;
DROP TABLE IF EXISTS stg_fund_db.audit_table;
CREATE TABLE stg_fund_db.audit_table
(
    id              INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    start_load_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    end_load_date   TIMESTAMP,
    task            VARCHAR(256),
    is_stored       BOOL      NOT NULL
);