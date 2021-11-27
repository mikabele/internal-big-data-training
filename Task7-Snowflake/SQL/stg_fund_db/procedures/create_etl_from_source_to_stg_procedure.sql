USE stg_fund_db;
CREATE OR REPLACE PROCEDURE usp_etl_from_source_to_stg(load_date VARCHAR,market VARCHAR)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS $$
    var stat = snowflake.createStatement({ sqlText:
    "MERGE INTO stg_fund_db.public.stg_fund_table target\
    USING (SELECT TO_TIMESTAMP(SUBSTRING(`time`, 1, 19)) AS `time`,\
           CAST(REPLACE(`open`, ',', '.') AS DECIMAL(7, 3)) AS `open`,\
           CAST(REPLACE(`close`, ',', '.') AS DECIMAL(7, 3)) AS `close`,\
           ? AS market,\
           TO_TIMESTAMP(?) AS loadDate\
    FROM landing_fund_db.public.temp_fund_table) AS source\
    ON target.market = source.market AND target.`time` = source.`time`\
    WHEN MATCHED THEN\
        UPDATE SET target.`open` = source.`open`,\
            target.`close` = source.`close`,\
            target.loadDate = source.loadDate\
    WHEN NOT MATCHED THEN\
        INSERT (`time`,`open`,`close`,market,loadDate)\
        VALUES (source.`time`,source.`open`,source.`close`,source.market,source.loadDate)",
        binds: [MARKET,LOAD_DATE]
     })
     stat.execute()
    return true
$$