USE landing_fund_db;
DROP TABLE IF EXISTS landing_fund_db.audit_table;
CREATE TABLE landing_fund_db.audit_table
(
    id                     INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    filename               VARCHAR(256) NOT NULL,
    `checksum`             VARCHAR(64)  NOT NULL,
    start_load_date        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    market                 VARCHAR(20)  NOT NULL,
    task                   VARCHAR(256),
    end_load_date          TIMESTAMP,
    count_of_inserted_rows INT UNSIGNED,
    is_stored              BOOL         NOT NULL
);