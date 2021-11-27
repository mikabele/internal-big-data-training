USE stg_fund_db;
CREATE OR REPLACE PROCEDURE usp_calculate_percents_per_month(type_of_load VARCHAR,market VARCHAR,load_date VARCHAR)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS $$
    var queryString = "MERGE INTO stg_fund_db.public.percents_per_month_table target\
                        USING (WITH success_load_dates_cte AS\
                       (\
                         SELECT startLoadDate\
                         FROM audit_db.public.stg_audit_table\
                         WHERE endLoadDate IS NOT NULL AND market = '"+MARKET+"'\
                       )"
    var rs = snowflake.execute({ sqlText: "SELECT MAX(startLoadDate) AS last\
                  FROM audit_db.public.fund_audit_table\
                  WHERE endLoadDate IS NOT NULL"})

    var lastUpdateDate = null
    if(rs.getRowCount() > 0) {
        rs.next()
        lastUpdateDate = rs.getColumnValueAsString(1)
    }
    if (TYPE_OF_LOAD == "full" || lastUpdateDate == null) {
        queryString += ",\
                         new_data_cte AS\
                         (\
                            SELECT YEAR(`time`) AS `year`,MONTH(`time`) AS `month`,`time`,`open`,`close`,market\
                            FROM stg_fund_db.public.stg_fund_table\
                         )"
    }
    else {

        queryString += ",\
                        months_to_update_cte AS \
                        (\
                          SELECT DISTINCT YEAR(`time`) AS `year`,MONTH(`time`) AS `month`\
                          FROM stg_fund_db.public.stg_fund_table\
                          WHERE loadDate IN (SELECT * FROM success_load_dates_cte) AND loadDate > '"+lastUpdateDate+"' AND market = '"+MARKET+"'\
                        ),\
                        new_data_cte AS\
                        (\
                          SELECT YEAR(`time`) AS `year`,MONTH(`time`) AS `month`,`time`,`open`,`close`,market\
                          FROM stg_fund_db.public.stg_fund_table AS stg\
                          INNER JOIN months_to_update_cte AS mtu\
                          ON YEAR(stg.`time`)=mtu.`year` AND MONTH(stg.`time`)=mtu.`month`\
                          WHERE loadDate IN (SELECT * FROM success_load_dates_cte)\
                        )"
    }
    queryString += ",\
                    open_close_cte AS\
                    (\
                     SELECT open_dates.`year`,\
                            open_dates.`month`,\
                            open_dates.`time`,\
                            open_dates.`open`,\
                            close_dates.`close`\
                     FROM (\
                              SELECT `year`, `month`, `time`, `open`, `close`\
                              FROM new_data_cte\
                              WHERE `time` IN\
                                    (\
                                        SELECT MIN(`time`)\
                                        FROM new_data_cte\
                                        GROUP BY `year`, `month`\
                                    )\
                          ) as open_dates\
                              INNER JOIN\
                          (\
                              SELECT `year`, `month`, `time`, `open`, `close`\
                              FROM new_data_cte\
                              WHERE `time` IN\
                                    (\
                                        SELECT MAX(`time`)\
                                        FROM new_data_cte\
                                        GROUP BY `year`, `month`\
                                    )\
                          ) as close_dates\
                          ON open_dates.`year` = close_dates.`year` AND open_dates.`month` = close_dates.`month`\
                    ),\
                     previous_close_cte AS\
                    (\
                     SELECT `time`,\
                            `year`,\
                            `month`,\
                            `open`,\
                            `close`,\
                            COALESCE(LAG(`close`) OVER (PARTITION BY `year` ORDER BY `month`), `open`) as prev_close\
                     FROM open_close_cte\
                    ),\
                    monthly_percents_cte AS\
                    (\
                     SELECT `year`,\
                            `month`,\
                            (`close` - `open`) / `open` * 100 AS percent,\
                             'open' AS typeOfPreviousValue\
                     FROM previous_close_cte\
                     UNION ALL\
                     SELECT `year`,\
                            `month`,\
                            (`close` - prev_close) / prev_close * 100 AS percent,\
                            'prev' AS typeOfPreviousValue\
                     FROM previous_close_cte\
                    )\
                    SELECT * FROM monthly_percents_cte ) AS source\
                    ON source.`year`=target.`year`  AND source.`month`=target.`month` AND target.market = '"+MARKET+"' AND source.typeOfPreviousValue = target.typeOfPreviousValue\
                    WHEN MATCHED THEN\
                      UPDATE SET target.percent=source.percent,\
                                 target.loadDate = '"+LOAD_DATE+"'\
                    WHEN NOT MATCHED THEN\
                      INSERT (`year`,`month`,percent,typeOfPreviousValue,loadDate, market)\
                      VALUES (source.`year`,source.`month`,source.percent,source.typeOfPreviousValue,'"+LOAD_DATE+"','"+MARKET+"')"
    snowflake.execute({sqlText : queryString})

    return true
$$