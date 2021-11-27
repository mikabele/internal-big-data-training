USE stg_fund_db;
CREATE PROCEDURE `usp_clear_fund`(IN is_full BOOL)
BEGIN
    DECLARE start_timestamp TIMESTAMP;
    DECLARE end_timestamp TIMESTAMP;

    IF is_full THEN
        TRUNCATE TABLE stg_fund_db.stg_fund_table;

        UPDATE stg_fund_db.audit_table
        SET is_stored= FALSE
        WHERE is_stored = TRUE;

        SET start_timestamp = CURRENT_TIMESTAMP();

        INSERT INTO stg_fund_db.audit_table(start_load_date, end_load_date, task, is_stored)
        VALUES (start_timestamp, NULL, 'clear data from landing_fund_table', FALSE);

        INSERT INTO stg_fund_db.stg_fund_table(`time`, `open`, `close`, load_date, market)
        SELECT TIMESTAMP(SUBSTRING(`time`, 1, 19)),
               CAST(REPLACE(`open`, ',', '.') AS DECIMAL(7, 3)),
               CAST(REPLACE(`close`, ',', '.') AS DECIMAL(7, 3)),
               start_timestamp,
               la.market
        FROM landing_fund_db.landing_fund_table as la
        WHERE load_date IN (SELECT start_load_date
                            FROM landing_fund_db.audit_table
                            WHERE end_load_date IS NOT NULL);


        SET end_timestamp = CURRENT_TIMESTAMP();

        UPDATE stg_fund_db.audit_table
        SET end_load_date=end_timestamp,
            is_stored= TRUE
        WHERE start_load_date = start_timestamp;
    ELSE
        SET start_timestamp = CURRENT_TIMESTAMP();

        INSERT INTO stg_fund_db.audit_table(start_load_date, end_load_date, task, is_stored)
        VALUES (start_timestamp, NULL, 'clear data from landing_fund_table', FALSE);

        REPLACE stg_fund_db.stg_fund_table(`time`, `open`, `close`, load_date, market)
        SELECT TIMESTAMP(SUBSTRING(`time`, 1, 19)),
               CAST(REPLACE(`open`, ',', '.') AS DECIMAL(7, 3)),
               CAST(REPLACE(`close`, ',', '.') AS DECIMAL(7, 3)),
               start_timestamp,
               la.market
        FROM landing_fund_db.landing_fund_table as la
        WHERE load_date IN (SELECT start_load_date
                            FROM landing_fund_db.audit_table
                            WHERE end_load_date IS NOT NULL)
          AND load_date > (
            SELECT COALESCE(MAX(start_load_date), FROM_UNIXTIME(0))
            FROM stg_fund_db.audit_table
            WHERE is_stored = TRUE
        );

        SET end_timestamp = CURRENT_TIMESTAMP();

        UPDATE stg_fund_db.audit_table
        SET end_load_date=end_timestamp,
            is_stored= TRUE
        WHERE start_load_date = start_timestamp;
    END IF;
END;