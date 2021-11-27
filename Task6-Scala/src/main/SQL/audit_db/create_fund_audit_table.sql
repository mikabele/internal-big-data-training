DROP TABLE IF EXISTS audit_db.fund_audit_table

CREATE TABLE audit_db.fund_audit_table
(
    id                     INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    startLoadDate          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    endLoadDate            TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    task                   VARCHAR(255),
    market                 VARCHAR(20)
);