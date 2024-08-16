create or replace database GLOBALBANK;

create or replace schema GLOBALBANK.raw_data;

create or replace schema GLOBALBANK.transform_data;

create or replace schema GLOBALBANK.analytics;

create or replace schema GLOBALBANK.security;


----create raw tables in raw schema

create or replace table GLOBALBANK.raw_data.transacation_raw(
transaction_id STRING,
 customer_id STRING,
 transaction_date TIMESTAMP_NTZ,
 amount FLOAT,
 currency STRING,
 transaction_type STRING,
 channel STRING,
 merchant_name STRING,
 merchant_category STRING,
 location_country STRING,
 location_city STRING,
 is_flagged BOOLEAN
);

create or replace table GLOBALBANK.raw_data.customer_raw(
 customer_id STRING,
 first_name STRING,
 last_name STRING,
 date_of_birth DATE,
 gender STRING,
 email STRING,
 phone_number STRING,
 address STRING,
 city STRING,
 country STRING,
 occupation STRING,
 income_bracket STRING,
 customer_since DATE
);



create or replace table GLOBALBANK.raw_data.account_raw(
account_id STRING,
 customer_id STRING,
 account_type STRING,
 account_status STRING,
 open_date DATE,
 current_balance FLOAT,
 currency STRING,
 credit_limit FLOAT
);

create or replace table GLOBALBANK.raw_data.credit_raw(
 customer_id STRING,
 credit_score INT,
 number_of_credit_accounts INT,
 total_credit_limit FLOAT,
 total_credit_used FLOAT,
 number_of_late_payments INT,
 bankruptcies INT
);


create or replace table GLOBALBANK.raw_data.watchlist(
 entity_id STRING,
 entity_name STRING,
 entity_type STRING,
 risk_category STRING,
 listed_date DATE,
 source STRING);


create or replace STORAGE INTEGRATION my_s3_integration
  type = External_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::975050369468:role/sami-new-role'
  storage_allowed_locations = ('s3://sami-s3-bucket/Data/');

desc STORAGE INTEGRATION my_s3_integration;


create or replace file format MY_CSV_FORMAT
-- FIELD_DELIMITER =','
-- RECORD_DELIMITER ='/n'
type = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
SKIP_HEADER = 1;

create or replace stage GLOBALBANK.raw_data.my_s3_stage 
STORAGE_INTEGRATION = my_s3_integration
URL = 's3://sami-s3-bucket/Data/'
file_format = MY_CSV_FORMAT;

list @GLOBALBANK.raw_data.my_s3_stage;

create or replace pipe transacation_raw_pipe
auto_ingest = true
as
COPY INTO GLOBALBANK.raw_data.transacation_raw
FROM @GLOBALBANK.raw_data.my_s3_stage/transaction_data.csv
FILE_FORMAT = MY_CSV_FORMAT
ON_ERROR = 'CONTINUE';
--VALIDATION_MODE = RETURN_ALL_ERRORS;;

alter pipe transacation_raw_pipe refresh;

select * from transacation_raw;

select SYSTEM$PIPE_STATUS( 'transacation_raw_pipe' );

-- select * from GLOBALBANK.raw_data.transacation_raw;where transaction_id='00a7b4c5-09c1-4e01-a8e3-3fc94e00912c';

-- select distinct transaction_id from transacation_raw;

-- truncate table transacation_raw;

-- select * from GLOBALBANK.raw_data.transacation_raw where is_flagged = 'true';

create or replace pipe customer_raw_pipe
auto_ingest = true
as
COPY INTO GLOBALBANK.raw_data.customer_raw
FROM @GLOBALBANK.raw_data.my_s3_stage/customer_data.csv
FILE_FORMAT = MY_CSV_FORMAT
ON_ERROR = 'CONTINUE';
--VALIDATION_MODE = RETURN_ALL_ERRORS;

alter pipe customer_raw_pipe refresh;

select * from customer_raw;



show pipes;

create or replace pipe account_raw_pipe
auto_ingest = true
as
COPY INTO GLOBALBANK.raw_data.account_raw
FROM @GLOBALBANK.raw_data.my_s3_stage/account_data.csv
FILE_FORMAT = (FORMAT_NAME = MY_CSV_FORMAT)
ON_ERROR = 'CONTINUE';
--VALIDATION_MODE = RETURN_ALL_ERRORS;

alter pipe account_raw_pipe refresh;

select * from GLOBALBANK.raw_data.account_raw;



create or replace pipe credit_raw_pipe
auto_ingest = true
as
COPY INTO GLOBALBANK.raw_data.credit_raw
FROM @GLOBALBANK.raw_data.my_s3_stage/credit_data.csv
FILE_FORMAT = (FORMAT_NAME = MY_CSV_FORMAT)
ON_ERROR = 'CONTINUE';
--VALIDATION_MODE = RETURN_ALL_ERRORS;

alter pipe credit_raw_pipe refresh;

select * from GLOBALBANK.raw_data.credit_raw;




create or replace pipe watchlist_raw_pipe
auto_ingest = true
as
COPY INTO GLOBALBANK.raw_data.watchlist
FROM @GLOBALBANK.raw_data.my_s3_stage/watchlist_data.csv
FILE_FORMAT = (FORMAT_NAME = MY_CSV_FORMAT)
ON_ERROR = 'CONTINUE';

alter pipe watchlist_raw_pipe refresh;

select * from GLOBALBANK.raw_data.watchlist;




SELECT * FROM INFORMATION_SCHEMA.PIPE_LOAD_HISTORY
WHERE PIPE_NAME = 'GLOBALBANK.raw_data.transacation_raw_pipe';

SELECT * FROM INFORMATION_SCHEMA.PIPE_LOAD_HISTORY
WHERE PIPE_NAME = 'GLOBALBANK.raw_data.customer_raw_pipe';



desc table GLOBALBANK.raw_data.transacation_raw;

select * from GLOBALBANK.raw_data.transacation_raw;

desc table customer_raw;


REVOKE APPLYBUDGET ON DATABASE GLOBALBANK FROM ROLE PC_DBT_ROLE;

grant all privileges on DATABASE GLOBALBANK to role PC_DBT_ROLE;

grant all privileges on schema RAW_DATA to role PC_DBT_ROLE;

grant select on all tables in schema RAW_DATA to role PC_DBT_ROLE;

GRANT SELECT ON FUTURE TABLES IN DATABASE GLOBALBANK TO ROLE PC_DBT_ROLE;  

show tables;

alter pipe customer_raw_pipe refresh;


select * from transacation_raw;

select * from customer_raw;

select * from account_raw;

select * from credit_raw;

select * from watchlist;

truncate table transacation_raw;
truncate table customer_raw;
truncate table account_raw;
truncate table credit_raw;
truncate table watchlist;





