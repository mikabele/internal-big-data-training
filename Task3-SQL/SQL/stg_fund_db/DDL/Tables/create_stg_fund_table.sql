USE stg_fund_db;
DROP TABLE IF EXISTS stg_fund_db.stg_fund_table;
CREATE TABLE stg_fund_db.stg_fund_table
(
    `time`    TIMESTAMP     NOT NULL,
    `open`    DECIMAL(7, 3) NOT NULL,
    `close`   DECIMAL(7, 3) NOT NULL,
    market    VARCHAR(20)   NOT NULL,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY ps_stg_fund_table (market, `time`)
);