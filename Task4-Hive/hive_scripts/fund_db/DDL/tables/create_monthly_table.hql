DROP TABLE IF EXISTS fund_db.monthly_table;
CREATE TABLE fund_db.monthly_table
(
    market                 VARCHAR(20),
    `year`                 VARCHAR(5),
    january                DECIMAL(7, 5),
    february               DECIMAL(7, 5),
    march                  DECIMAL(7, 5),
    april                  DECIMAL(7, 5),
    may                    DECIMAL(7, 5),
    june                   DECIMAL(7, 5),
    july                   DECIMAL(7, 5),
    august                 DECIMAL(7, 5),
    september              DECIMAL(7, 5),
    october                DECIMAL(7, 5),
    november               DECIMAL(7, 5),
    december               DECIMAL(7, 5),
    total                  DECIMAL(7, 5),
    type_of_previous_value BOOLEAN,
    update_time            TIMESTAMP
);