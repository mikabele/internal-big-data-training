USE fund_db;
DROP TABLE IF EXISTS fund_db.audit_table;
CREATE TABLE fund_db.audit_table
(
    id         INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    start_date TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    end_date   TIMESTAMP,
    market     VARCHAR(20) NOT NULL,
    task       VARCHAR(256),
    is_stored  BOOL        NOT NULL
);