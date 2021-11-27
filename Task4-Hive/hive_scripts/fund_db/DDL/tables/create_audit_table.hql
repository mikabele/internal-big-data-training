DROP TABLE IF EXISTS fund_db.audit_table;
CREATE TABLE fund_db.audit_table
(
    start_date TIMESTAMP,
    end_date   TIMESTAMP,
    market     VARCHAR(20),
    task       VARCHAR(256),
    is_stored  BOOLEAN
);