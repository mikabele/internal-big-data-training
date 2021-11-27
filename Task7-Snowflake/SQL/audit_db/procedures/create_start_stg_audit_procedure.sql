USE audit_db;

CREATE OR REPLACE PROCEDURE usp_start_stg_audit(filename VARCHAR,check_sum VARCHAR , start_load_date VARCHAR,market VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    var queryString = "INSERT INTO audit_db.public.stg_audit_table(filename,checksum,startLoadDate,endLoadDate,task,market,countOfInsertedRows)\
                        VALUES ('"+FILENAME+"','"+CHECK_SUM+"','"+START_LOAD_DATE+"',NULL,'load data from source to staging','"+MARKET+"',NULL)"
    snowflake.execute({sqlText : queryString})
    return queryString
$$;