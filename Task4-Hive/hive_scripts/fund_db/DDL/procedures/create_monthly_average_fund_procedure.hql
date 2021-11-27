USE fund_db;
CREATE PROCEDURE usp_monthly_average_fund(IN open_close BOOL)
BEGIN
    WITH open_close_cte AS
    (
        SELECT open_dates.year,open_dates.month,open_dates.day,open_dates.time, open_dates.open, close_dates.close
        FROM
        (
            SELECT year,month,day,time,open,close
            FROM fund_db.fund_table
            WHERE time IN
            (
                SELECT MIN(time)
                FROM fund_db.fund_table
                GROUP BY year,month
            )
        ) as open_dates
        INNER JOIN
        (
            SELECT year,month,day,time,open,close
            FROM fund_db.fund_table
            WHERE time IN
            (
                SELECT MAX(time)
                FROM fund_db.fund_table
                GROUP BY year,month
            )
        ) as close_dates
        ON open_dates.year=close_dates.year AND open_dates.month=close_dates.month
    ),
    close_previous_close_cte AS
    (
        SELECT time,
            year,
            month,
            open,
            close,
            COALESCE(LAG(close) OVER (PARTITION BY year ORDER BY month), open) AS prev_close
        FROM open_close_cte
    ),
    monthly_percents_cte AS
    (
        SELECT year,month, IF
        (
            open_close,
            (close-open)/open*100,
            (close-prev_close)/prev_close*100
        ) as percents
        FROM close_previous_close_cte
    ),
    result_cte AS
    (
        SELECT  year,
                SUM(IF(month=1,percents,NULL)) AS January,
                SUM(IF(month=2,percents,NULL)) AS February,
                SUM(IF(month=3,percents,NULL)) AS March,
                SUM(IF(month=4,percents,NULL)) AS April,
                SUM(IF(month=5,percents,NULL)) AS May,
                SUM(IF(month=6,percents,NULL)) AS June,
                SUM(IF(month=7,percents,NULL)) AS July,
                SUM(IF(month=8,percents,NULL)) AS August,
                SUM(IF(month=9,percents,NULL)) AS September,
                SUM(IF(month=10,percents,NULL)) AS October,
                SUM(IF(month=11,percents,NULL)) AS November,
                SUM(IF(month=12,percents,NULL)) AS December,
                SUM(percents) AS Total
        FROM monthly_percents_cte
        GROUP BY year
    )
    SELECT *
    FROM result_cte
    UNION ALL
    SELECT 0,
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
        AVG(Total)
    FROM result_cte;
END;
