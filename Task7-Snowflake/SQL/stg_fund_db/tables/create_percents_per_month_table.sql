USE stg_fund_db;

CREATE OR REPLACE TABLE percents_per_month_table
(
  `year`              INT NOT NULL,
  `month`             INT NOT NULL,
  percent             DECIMAL(7,3),
  typeOfPreviousValue VARCHAR(10) NOT NULL,
  loadDate            TIMESTAMP,
  market              VARCHAR(20) NOT NULL,
  PRIMARY KEY (`year`,`month`,typeOfPreviousValue,market)
);