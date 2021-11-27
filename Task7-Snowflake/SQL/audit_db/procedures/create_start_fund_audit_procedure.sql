USE audit_db;

CREATE OR REPLACE PROCEDURE usp_start_fund_audit(start_load_date VARCHAR,market VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    var queryString = "INSERT INTO audit_db.public.fund_audit_table(startLoadDate,endLoadDate,task,market)\
                       VALUES ('"+START_LOAD_DATE+"',NULL,'load data from stg to destination','"+MARKET+"')"
    snowflake.execute({sqlText : queryString})
    return queryString
$$;