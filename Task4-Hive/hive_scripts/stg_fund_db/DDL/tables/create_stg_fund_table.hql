DROP TABLE IF EXISTS stg_fund_db.stg_fund_table;
CREATE TABLE stg_fund_db.stg_fund_table
(
    `time`    TIMESTAMP ,
    `open`    DECIMAL(7, 3),
    `close`   DECIMAL(7, 3),
    market    VARCHAR(20),
    load_date TIMESTAMP
);