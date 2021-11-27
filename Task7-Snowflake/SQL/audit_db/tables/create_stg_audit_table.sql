USE audit_db;

CREATE OR REPLACE TABLE stg_audit_table
(
    id                     INT AUTOINCREMENT(1,1),
    filename               VARCHAR(255),
    checksum               VARCHAR(64),
    startLoadDate          TIMESTAMP,
    endLoadDate            TIMESTAMP,
    task                   VARCHAR(255),
    market                 VARCHAR(20),
    countOfInsertedRows    BIGINT,
    PRIMARY KEY (id)
);