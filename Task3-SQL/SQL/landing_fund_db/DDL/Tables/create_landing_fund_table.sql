USE landing_fund_db;
DROP TABLE IF EXISTS landing_fund_db.landing_fund_table;
CREATE TABLE landing_fund_db.landing_fund_table
(
    `time`    VARCHAR(19) NOT NULL,
    `open`    VARCHAR(8)  NOT NULL,
    `high`    VARCHAR(8)  NOT NULL,
    `low`     VARCHAR(8)  NOT NULL,
    `close`   VARCHAR(8)  NOT NULL,
    volume    VARCHAR(7)  NOT NULL,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    market    VARCHAR(20) NOT NULL,
    PRIMARY KEY pk_stg_landing_table (market, `time`)
);