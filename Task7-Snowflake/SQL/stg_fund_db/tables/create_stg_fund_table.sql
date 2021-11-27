USE stg_fund_db;

CREATE OR REPLACE TABLE stg_fund_table
(
    `time`    TIMESTAMP     NOT NULL,
    `open`    DECIMAL(7, 3) NOT NULL,
    `close`   DECIMAL(7, 3) NOT NULL,
    market    VARCHAR(20)   NOT NULL,
    loadDate  TIMESTAMP     NOT NULL,
    PRIMARY KEY (market, `time`)
);