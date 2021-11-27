USE audit_db;
CREATE OR REPLACE PROCEDURE usp_end_stg_audit(check_sum VARCHAR , start_load_date VARCHAR,count_of_rows FLOAT)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    var queryString = "UPDATE audit_db.public.stg_audit_table\
                            SET endLoadDate = CURRENT_TIMESTAMP(),\
                                countOfInsertedRows = "+COUNT_OF_ROWS+"\
                            WHERE startLoadDate = '"+START_LOAD_DATE+"' AND checksum = '"+CHECK_SUM+"'"
    snowflake.execute({sqlText : queryString})
    return queryString
$$;