DROP TABLE IF EXISTS landing_fund_db.tmp_fund_table;
CREATE TABLE landing_fund_db.tmp_fund_table
(
    `time`    VARCHAR(19),
    `open`    VARCHAR(8),
    `high`    VARCHAR(8),
    `low`     VARCHAR(8),
    `close`   VARCHAR(8),
    volume    VARCHAR(7),
    load_date TIMESTAMP,
    market    VARCHAR(20)
);