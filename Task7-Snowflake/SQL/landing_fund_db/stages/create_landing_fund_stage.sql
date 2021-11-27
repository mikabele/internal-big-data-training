USE landing_fund_db;

CREATE OR REPLACE STAGE landing_fund_stage
FILE_FORMAT = (TYPE = 'CSV'
               ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
               FIELD_DELIMITER = ';'
               RECORD_DELIMITER = '\n'
               SKIP_HEADER = 1);