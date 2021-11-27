DROP TABLE IF EXISTS audit_db.stg_audit_table

CREATE TABLE audit_db.stg_audit_table
(
    id                     INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    filename               VARCHAR(255),
    checksum               VARCHAR(64),
    startLoadDate          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    endLoadDate            TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    task                   VARCHAR(255),
    market                 VARCHAR(20),
    countOfInsertedRows    BIGINT
);