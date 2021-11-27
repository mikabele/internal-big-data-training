USE audit_db;
CREATE OR REPLACE PROCEDURE usp_end_fund_audit(start_load_date VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    var queryString = "UPDATE audit_db.public.fund_audit_table\
                            SET endLoadDate = CURRENT_TIMESTAMP()\
                            WHERE startLoadDate = '"+START_LOAD_DATE+"'"
    snowflake.execute({sqlText : queryString})
    return queryString
$$;