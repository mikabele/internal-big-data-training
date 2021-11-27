DROP TABLE IF EXISTS landing_fund_db.audit_table;
CREATE TABLE landing_fund_db.audit_table
(
    filename               VARCHAR(256),
    `checksum`             VARCHAR(64),
    start_load_date        TIMESTAMP,
    market                 VARCHAR(20),
    task                   VARCHAR(256),
    end_load_date          TIMESTAMP,
    count_of_inserted_rows INT,
    is_stored              BOOLEAN
);