USE landing_fund_db;

CREATE OR REPLACE TABLE temp_fund_table
(
    `time`    VARCHAR(19) NOT NULL,
    `open`    VARCHAR(8)  NOT NULL,
    `high`    VARCHAR(8)  NOT NULL,
    `low`     VARCHAR(8)  NOT NULL,
    `close`   VARCHAR(8)  NOT NULL,
    volume    VARCHAR(7)  NOT NULL,
    PRIMARY KEY (`time`)
);