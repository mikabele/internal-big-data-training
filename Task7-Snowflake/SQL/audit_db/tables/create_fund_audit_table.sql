USE audit_db;

CREATE OR REPLACE TABLE fund_audit_table
(
    id                  INT AUTOINCREMENT (1,1),
    startLoadDate       TIMESTAMP NOT NULL,
    endLoadDate         TIMESTAMP,
    market              VARCHAR(20)  NOT NULL,
    task                VARCHAR(256),
    PRIMARY KEY (id)
);