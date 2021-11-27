USE fund_db;
CREATE OR REPLACE PROCEDURE usp_monthly_overview(type_of_load VARCHAR,market VARCHAR,load_date VARCHAR)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
    var queryString = "MERGE INTO fund_db.public.monthly_table target"
    if (TYPE_OF_LOAD == "full") {
        queryString += " USING (\
                                WITH result_cte AS \
                                (\
                                  SELECT `year`,\
                                    SUM(IFF(`month` = 1, percent, NULL))  AS January,\
                                    SUM(IFF(`month` = 2, percent, NULL))  AS February,\
                                    SUM(IFF(`month` = 3, percent, NULL))  AS March,\
                                    SUM(IFF(`month` = 4, percent, NULL))  AS April,\
                                    SUM(IFF(`month` = 5, percent, NULL))  AS May,\
                                    SUM(IFF(`month` = 6, percent, NULL))  AS June,\
                                    SUM(IFF(`month` = 7, percent, NULL))  AS July,\
                                    SUM(IFF(`month` = 8, percent, NULL))  AS August,\
                                    SUM(IFF(`month` = 9, percent, NULL))  AS September,\
                                    SUM(IFF(`month` = 10,percent, NULL)) AS October,\
                                    SUM(IFF(`month` = 11, percent, NULL)) AS November,\
                                    SUM(IFF(`month` = 12, percent, NULL)) AS December,\
                                    SUM(percent)                         AS Total,\
                                    typeOfPreviousValue\
                                  FROM stg_fund_db.public.percents_per_month_table\
                                  GROUP BY typeOfPreviousValue, `year`\
                                )\
                                SELECT '"+MARKET+"' AS market,\
                                     CAST(`year` AS CHAR(4)) AS `year`,\
                                    January,\
                                    February,\
                                    March,\
                                    April,\
                                    May,\
                                    June,\
                                    July,\
                                    August,\
                                    September,\
                                    October,\
                                    November,\
                                    December,\
                                    Total,\
                                    typeOfPreviousValue,\
                                    '"+LOAD_DATE+"' AS loadDate\
                                FROM result_cte\
                              ) AS source"
    }
    else {
        queryString += " USING (\
                        WITH years_to_update_cte AS\
                        (\
                          SELECT DISTINCT `year`\
                          FROM stg_fund_db.public.percents_per_month_table\
                          WHERE loadDate = '"+LOAD_DATE+"'\
                        ),\
                        result_cte AS \
                                (\
                                  SELECT `year`,\
                                    SUM(IFF(`month` = 1, percent, NULL))  AS january,\
                                    SUM(IFF(`month` = 2, percent, NULL))  AS february,\
                                    SUM(IFF(`month` = 3, percent, NULL))  AS march,\
                                    SUM(IFF(`month` = 4, percent, NULL))  AS april,\
                                    SUM(IFF(`month` = 5, percent, NULL))  AS may,\
                                    SUM(IFF(`month` = 6, percent, NULL))  AS june,\
                                    SUM(IFF(`month` = 7, percent, NULL))  AS july,\
                                    SUM(IFF(`month` = 8, percent, NULL))  AS august,\
                                    SUM(IFF(`month` = 9, percent, NULL))  AS september,\
                                    SUM(IFF(`month` = 10, percent, NULL)) AS october,\
                                    SUM(IFF(`month` = 11, percent, NULL)) AS november,\
                                    SUM(IFF(`month` = 12, percent, NULL)) AS december,\
                                    SUM(percent)                         AS total,\
                                    typeOfPreviousValue\
                                  FROM (\
                                    SELECT `year`,`month`,percent,typeOfPreviousValue\
                                    FROM stg_fund_db.public.percents_per_month_table\
                                    WHERE `year` IN (SELECT * FROM years_to_update_cte) AND market = '"+MARKET+"'\
                                    )\
                                  GROUP BY typeOfPreviousValue, `year`\
                                )\
                                SELECT '"+MARKET+"' AS market,\
                                     CAST(`year` AS CHAR(4)) AS `year`,\
                                    january,\
                                    february,\
                                    march,\
                                    april,\
                                    may,\
                                    june,\
                                    july,\
                                    august,\
                                    september,\
                                    october,\
                                    november,\
                                    december,\
                                    total,\
                                    typeOfPreviousValue,\
                                    '"+LOAD_DATE+"' AS loadDate\
                                FROM result_cte\
                                ) AS source"
    }
    queryString += " ON target.`year`=source.`year` AND target.market = source.market AND target.typeOfPreviousValue = source.typeOfPreviousValue\
                    WHEN MATCHED THEN\
                        UPDATE SET target.january = source.january,\
                        target.february = source.february,\
                        target.march = source.march,\
                        target.april = source.april,\
                        target.may = source.may,\
                        target.june = source.june,\
                        target.july = source.july,\
                        target.august = source.august,\
                        target.september = source.september,\
                        target.october = source.october,\
                        target.november = source.november,\
                        target.december = source.december,\
                        target.total = source.total,\
                        target.loadDate = source.loadDate\
                    WHEN NOT MATCHED THEN\
                        INSERT (market,`year`,january,february,march,april,may,september,october,november,december,total,typeOfPreviousValue,loadDate)\
                        VALUES ('"+MARKET+"',source.`year`,source.january,source.february,source.march,source.april,source.may,source.september,source.october,source.november,source.december,source.total,source.typeOfPreviousValue,loadDate)"
    snowflake.execute({sqlText:queryString})
    queryString = "MERGE INTO fund_db.public.monthly_table target\
                   USING (\
                        SELECT  '"+MARKET+"' AS market,\
                        'total' AS `year`,\
                        AVG(january) AS january,\
                        AVG(february) AS february,\
                        AVG(march) AS march,\
                        AVG(april) AS april,\
                        AVG(may) AS may,\
                        AVG(june) AS june,\
                        AVG(july) AS july,\
                        AVG(august) AS august,\
                        AVG(september) AS september,\
                        AVG(october) AS october,\
                        AVG(november) AS november,\
                        AVG(december) AS december,\
                        AVG(total) AS total,\
                        typeOfPreviousValue,\
                        '"+LOAD_DATE+"' AS loadDate\
                        FROM fund_db.public.monthly_table\
                        WHERE market = '"+MARKET+"'\
                        GROUP BY typeOfPreviousValue\
                        ) AS source\
                        ON target.`year`=source.`year` AND target.market = source.market AND target.typeOfPreviousValue = source.typeOfPreviousValue\
                        WHEN MATCHED THEN\
                            UPDATE SET target.january = source.january,\
                                       target.february = source.february,\
                                       target.march = source.march,\
                                       target.april = source.april,\
                                       target.may = source.may,\
                                       target.june = source.june,\
                                       target.july = source.july,\
                                       target.august = source.august,\
                                       target.september = source.september,\
                                       target.october = source.october,\
                                       target.november = source.november,\
                                       target.december = source.december,\
                                       target.total = source.total,\
                                       target.loadDate = source.loadDate\
                        WHEN NOT MATCHED THEN\
                            INSERT (market,`year`,january,february,march,april,may,september,october,november,december,total,typeOfPreviousValue,loadDate)\
                            VALUES ('"+MARKET+"',source.`year`,source.january,source.february,source.march,source.april,source.may,source.september,source.october,source.november,source.december,source.total,source.typeOfPreviousValue,loadDate)"
    snowflake.execute({sqlText:queryString})
    return true
$$