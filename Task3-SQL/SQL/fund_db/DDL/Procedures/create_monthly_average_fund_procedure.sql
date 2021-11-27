USE fund_db;
DROP PROCEDURE IF EXISTS usp_monthly_average_fund;
DELIMITER //
CREATE PROCEDURE `usp_monthly_average_fund`(IN is_full BOOL, IN open_close BOOl, IN in_market VARCHAR(256),
                                            IN task VARCHAR(256))
BEGIN
    DECLARE start_timestamp TIMESTAMP;

    SET start_timestamp = CURRENT_TIMESTAMP();

    INSERT INTO fund_db.audit_table(start_date, end_date, market, task, is_stored)
    VALUES (start_timestamp, NULL, in_market, task, FALSE);
    IF is_full THEN
        TRUNCATE TABLE fund_db.monthly_table;

        INSERT INTO fund_db.monthly_table
        WITH new_data_cte AS
                 (
                     SELECT `time`, `open`, `close`
                     FROM stg_fund_db.stg_fund_table
                     WHERE market = in_market
                 ),
             open_close_cte AS
                 (
                     SELECT open_dates.`year`,
                            `open_dates`.`month`,
                            open_dates.`time`,
                            open_dates.`open`,
                            close_dates.`close`
                     FROM (
                              SELECT YEAR(`time`) as `year`, MONTH(`time`) as `month`, `time`, `open`, `close`
                              FROM new_data_cte
                              WHERE `time` IN
                                    (
                                        SELECT MIN(`time`)
                                        FROM new_data_cte
                                        GROUP BY YEAR(`time`), MONTH(`time`)
                                    )
                          ) as open_dates
                              INNER JOIN
                          (
                              SELECT YEAR(`time`) as `year`, MONTH(`time`) as `month`, `time`, `open`, `close`
                              FROM new_data_cte
                              WHERE `time` IN
                                    (
                                        SELECT MAX(`time`)
                                        FROM new_data_cte
                                        GROUP BY YEAR(`time`), MONTH(`time`)
                                    )
                          ) as close_dates
                          ON open_dates.`year` = close_dates.`year` AND open_dates.`month` = close_dates.`month`
                 ),
             close_previous_close_cte AS
                 (
                     SELECT `time`,
                            `year`,
                            `month`,
                            `open`,
                            `close`,
                            COALESCE(LAG(`close`) OVER (PARTITION BY `year` ORDER BY `month`), `open`) as prev_close
                     FROM open_close_cte
                 ),
             monthly_percents_cte AS
                 (
                     SELECT `year`,
                            `month`,
                            IF
                                (
                                    open_close,
                                    (`close` - `open`) / `open` * 100,
                                    (`close` - `prev_close`) / `prev_close` * 100
                                ) as percents
                     FROM close_previous_close_cte
                 ),
             result_cte AS
                 (
                     SELECT `year`,
                            SUM(IF(`month` = 1, `percents`, NULL))  AS January,
                            SUM(IF(`month` = 2, `percents`, NULL))  AS February,
                            SUM(IF(`month` = 3, `percents`, NULL))  AS March,
                            SUM(IF(`month` = 4, `percents`, NULL))  AS April,
                            SUM(IF(`month` = 5, `percents`, NULL))  AS May,
                            SUM(IF(`month` = 6, `percents`, NULL))  AS June,
                            SUM(IF(`month` = 7, `percents`, NULL))  AS July,
                            SUM(IF(`month` = 8, `percents`, NULL))  AS August,
                            SUM(IF(`month` = 9, `percents`, NULL))  AS September,
                            SUM(IF(`month` = 10, `percents`, NULL)) AS October,
                            SUM(IF(`month` = 11, `percents`, NULL)) AS November,
                            SUM(IF(`month` = 12, `percents`, NULL)) AS December,
                            SUM(`percents`)                         AS Total
                     FROM monthly_percents_cte
                     GROUP BY `year`
                 )
        SELECT in_market,
               CAST(`year` AS CHAR(4)),
               January,
               February,
               March,
               April,
               May,
               June,
               July,
               August,
               September,
               October,
               November,
               December,
               Total,
               open_close,
               start_timestamp
        FROM result_cte
        UNION ALL
        SELECT in_market,
               'total',
               AVG(January),
               AVG(February),
               AVG(March),
               AVG(April),
               AVG(May),
               AVG(June),
               AVG(July),
               AVG(August),
               AVG(September),
               AVG(October),
               AVG(November),
               AVG(December),
               AVG(Total),
               open_close,
               start_timestamp
        FROM result_cte;
    ELSE
        CREATE TABLE fund_db.temp_year_table
        (
            `year` SMALLINT NOT NULL
        );

        INSERT INTO fund_db.temp_year_table
        SELECT DISTINCT YEAR(`time`)
        FROM stg_fund_db.stg_fund_table
        WHERE load_date > (
            SELECT COALESCE(MAX(update_time), FROM_UNIXTIME(0))
            FROM fund_db.monthly_table
            WHERE type_of_previous_value = open_close
              AND market = in_market
        );

        DELETE
        FROM fund_db.monthly_table
        WHERE type_of_previous_value = open_close
          AND market = in_market
          AND `year` IN (
            SELECT CAST(`year` AS CHAR(5))
            FROM fund_db.temp_year_table
            UNION ALL
            SELECT 'total'
        );

        INSERT INTO fund_db.monthly_table
        WITH new_data_cte AS
                 (
                     SELECT `time`, `open`, `close`
                     FROM stg_fund_db.stg_fund_table
                     WHERE market = in_market
                       AND YEAR(`time`) IN (
                         SELECT `year`
                         FROM fund_db.temp_year_table
                     )
                 ),
             open_close_cte AS
                 (
                     SELECT open_dates.`year`,
                            `open_dates`.`month`,
                            open_dates.`time`,
                            open_dates.`open`,
                            close_dates.`close`
                     FROM (
                              SELECT YEAR(`time`) as `year`, MONTH(`time`) as `month`, `time`, `open`, `close`
                              FROM new_data_cte
                              WHERE `time` IN
                                    (
                                        SELECT MIN(`time`)
                                        FROM new_data_cte
                                        GROUP BY YEAR(`time`), MONTH(`time`)
                                    )
                          ) as open_dates
                              INNER JOIN
                          (
                              SELECT YEAR(`time`) as `year`, MONTH(`time`) as `month`, `time`, `open`, `close`
                              FROM new_data_cte
                              WHERE `time` IN
                                    (
                                        SELECT MAX(`time`)
                                        FROM new_data_cte
                                        GROUP BY YEAR(`time`), MONTH(`time`)
                                    )
                          ) as close_dates
                          ON open_dates.`year` = close_dates.`year` AND open_dates.`month` = close_dates.`month`
                 ),
             close_previous_close_cte AS
                 (
                     SELECT `time`,
                            `year`,
                            `month`,
                            `open`,
                            `close`,
                            COALESCE(LAG(`close`) OVER (PARTITION BY `year` ORDER BY `month`), `open`) as prev_close
                     FROM open_close_cte
                 ),
             monthly_percents_cte AS
                 (
                     SELECT `year`,
                            `month`,
                            IF
                                (
                                    open_close,
                                    (`close` - `open`) / `open` * 100,
                                    (`close` - `prev_close`) / `prev_close` * 100
                                ) as percents
                     FROM close_previous_close_cte
                 ),
             result_cte AS
                 (
                     SELECT `year`,
                            SUM(IF(`month` = 1, `percents`, NULL))  AS January,
                            SUM(IF(`month` = 2, `percents`, NULL))  AS February,
                            SUM(IF(`month` = 3, `percents`, NULL))  AS March,
                            SUM(IF(`month` = 4, `percents`, NULL))  AS April,
                            SUM(IF(`month` = 5, `percents`, NULL))  AS May,
                            SUM(IF(`month` = 6, `percents`, NULL))  AS June,
                            SUM(IF(`month` = 7, `percents`, NULL))  AS July,
                            SUM(IF(`month` = 8, `percents`, NULL))  AS August,
                            SUM(IF(`month` = 9, `percents`, NULL))  AS September,
                            SUM(IF(`month` = 10, `percents`, NULL)) AS October,
                            SUM(IF(`month` = 11, `percents`, NULL)) AS November,
                            SUM(IF(`month` = 12, `percents`, NULL)) AS December,
                            SUM(`percents`)                         AS Total
                     FROM monthly_percents_cte
                     GROUP BY `year`
                 )
        SELECT in_market,
               CAST(`year` AS CHAR(4)),
               January,
               February,
               March,
               April,
               May,
               June,
               July,
               August,
               September,
               October,
               November,
               December,
               Total,
               open_close,
               start_timestamp
        FROM result_cte;

        INSERT INTO fund_db.montly_table
        SELECT in_market,
               'total',
               AVG(January),
               AVG(February),
               AVG(March),
               AVG(April),
               AVG(May),
               AVG(June),
               AVG(July),
               AVG(August),
               AVG(September),
               AVG(October),
               AVG(November),
               AVG(December),
               AVG(Total),
               open_close,
               start_timestamp
        FROM result_cte
        WHERE market = in_market
          AND type_of_previous_value = open_close;

        DROP TABLE stg_fund_db.temp_year_table;
    END IF;


    UPDATE fund_db.audit_table
    SET end_date=CURRENT_TIMESTAMP(),
        is_stored= TRUE
    WHERE start_date = start_timestamp;
END;
//