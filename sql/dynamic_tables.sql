--- WAREHOUSE ---
-- create separate warehouse for dynamic tables for easier cost tracking
USE ROLE accountadmin;

 CREATE WAREHOUSE IF NOT EXISTS dynamic_wh
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

GRANT USAGE ON WAREHOUSE dynamic_wh TO ROLE sysadmin;

--- CONTEXT SETUP ---
USE ROLE sysadmin;
USE WAREHOUSE dynamic_wh;
USE DATABASE POC_DB;
CREATE SCHEMA IF NOT EXISTS POC_DB.POC2_DYNAMIC;
USE SCHEMA POC2_DYNAMIC;
ALTER SESSION SET TIMEZONE = 'Europe/Warsaw';


--- CREATE RAW LANDING TABLES ---
-- transactions
CREATE OR REPLACE TABLE raw_trans (
    trans_num_raw VARCHAR,
    trans_date_raw VARCHAR,
    card_num_raw VARCHAR,
    amount_raw VARCHAR,
    merchant_raw VARCHAR,
    category_raw VARCHAR,
    is_fraud_raw VARCHAR,
    -- meta columns
    meta_filename VARCHAR,
    meta_file_row_number INT,
    meta_load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CHANGE_TRACKING = TRUE;

-- clients
CREATE OR REPLACE TABLE raw_clients (
    card_num_raw VARCHAR,
    first_name_raw VARCHAR,
    last_name_raw VARCHAR,
    gender_raw VARCHAR,
    date_birth_raw VARCHAR,
    job_raw VARCHAR,
    street_raw VARCHAR,
    city_raw VARCHAR,
    state_raw VARCHAR,
    -- meta columns
    meta_filename VARCHAR,
    meta_file_row_number INT,
    meta_load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CHANGE_TRACKING = TRUE;

-- merchants
CREATE OR REPLACE TABLE raw_merch (
    merchant_raw VARCHAR,
    merchant_lat_raw VARCHAR,
    merchant_long_raw VARCHAR,
    -- meta columns
    meta_filename VARCHAR,
    meta_file_row_number INT,
    meta_load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CHANGE_TRACKING = TRUE;

--- CREATE FILE FORMAT ---
CREATE OR REPLACE FILE FORMAT ff_poc_csv
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  NULL_IF = ('', 'NULL', 'null')
  REPLACE_INVALID_CHARACTERS = TRUE;


--- CREATE EXTERAL STAGES: 1 stage per folder in S3 ---
CREATE OR REPLACE STAGE dynamic_stage_trans
  URL = 's3://s3-poc2-mb/poc2_dynamic/fraud_transactions/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = ff_poc_csv;

CREATE OR REPLACE STAGE dynamic_stage_client
  URL = 's3://s3-poc2-mb/poc2_dynamic/fraud_clients/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = ff_poc_csv;

CREATE OR REPLACE STAGE dynamic_stage_merch
  URL = 's3://s3-poc2-mb/poc2_dynamic/fraud_merchant/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = ff_poc_csv;

SHOW STAGES;

LIST @dynamic_stage_trans;
LIST @dynamic_stage_client;
LIST @dynamic_stage_merch;

--- CREATE PIPES: 1 pipe per stage ---
USE ROLE pipeadmin;

-- transactions pipe
CREATE OR REPLACE PIPE pipe_trans
  AUTO_INGEST = TRUE
AS
  COPY INTO raw_trans (
    trans_num_raw, 
    trans_date_raw,
    card_num_raw, 
    amount_raw,
    merchant_raw,
    category_raw, 
    is_fraud_raw, 
    meta_filename, 
    meta_file_row_number
  )
  FROM (
    SELECT 
      $1, $2, $3, $4, $5, $6, $7,
      METADATA$FILENAME,
      METADATA$FILE_ROW_NUMBER
    FROM @dynamic_stage_trans
  );

-- clients pipe
CREATE OR REPLACE PIPE pipe_client
  AUTO_INGEST = TRUE
AS
  COPY INTO raw_clients (
    card_num_raw,
    first_name_raw,
    last_name_raw,
    gender_raw,
    street_raw,
    city_raw,
    state_raw,
    job_raw,
    date_birth_raw,
    meta_filename,
    meta_file_row_number
  )
  FROM (
    SELECT 
      $1, $2, $3, $4, $5, $6, $7, $9, $10,
      METADATA$FILENAME,
      METADATA$FILE_ROW_NUMBER
    FROM @dynamic_stage_client
  );

-- merchants pipe
CREATE OR REPLACE PIPE pipe_merch
  AUTO_INGEST = TRUE
AS
  COPY INTO raw_merch (
    merchant_raw,
    merchant_lat_raw,
    merchant_long_raw,
    meta_filename,
    meta_file_row_number
  )
  FROM (
    SELECT 
      $2, $3, $4,
      METADATA$FILENAME,
      METADATA$FILE_ROW_NUMBER
    FROM @dynamic_stage_merch
  );

SHOW PIPES;
DESC PIPE pipe_trans;
DESC PIPE pipe_client; 
DESC PIPE pipe_merch;

-- refresh pipes
ALTER PIPE pipe_trans REFRESH;
ALTER PIPE pipe_client REFRESH;
ALTER PIPE pipe_merch REFRESH;

SELECT SYSTEM$PIPE_STATUS('PIPE_TRANS');
SELECT SYSTEM$PIPE_STATUS('PIPE_CLIENT');
SELECT SYSTEM$PIPE_STATUS('PIPE_MERCH');

-- quick check data landed
SELECT COUNT(*) FROM raw_trans;
SELECT COUNT(*) FROM raw_clients;
SELECT COUNT(*) FROM raw_merch;
SELECT * FROM raw_trans
ORDER BY meta_load_ts DESC LIMIT 10;
SELECT * FROM raw_clients
ORDER BY meta_load_ts DESC LIMIT 10;
SELECT * FROM raw_merch
ORDER BY meta_load_ts DESC LIMIT 10;




--- DYNAMIC TABLES ---

-- DYNAMIC TABLE STAGE 1: CLEAN-UP AND LOGIC --

-- DYNAMIC TABLE TRANSACTIONS 
-- cleaning types and basic validation
CREATE OR REPLACE DYNAMIC TABLE dt_transactions

-- transactions data (a zwlaszcza fraud data) is time sensitive
-- duze koszty, warehouse musi byc wbudzany bardzo czesto (praktycznie ciagle)
-- ale jest to uzasadnione ze wzgledu na charakter danych (kontekst biznesowy)
  TARGET_LAG = '1 minute'
  WAREHOUSE = dynamic_wh
AS
SELECT
  trans_num_raw AS trans_num,
  TRY_TO_TIMESTAMP(trans_date_raw, 'YYYY-MM-DD HH24:MI:SS') AS trans_ts,
  card_num_raw AS card_num,
  TRY_TO_DECIMAL(amount_raw, 18, 2) AS amount,
  merchant_raw AS merchant,
  category_raw AS category,
  TRY_TO_BOOLEAN(is_fraud_raw) AS is_fraud,
  meta_load_ts, -- zachowujemy metadane do pozniejszej analizy i debugowania

  -- quick validation
  CASE 
    WHEN TRY_TO_TIMESTAMP(trans_date_raw, 'YYYY-MM-DD HH24:MI:SS') IS NULL THEN 'INVALID_DATE'
    WHEN TRY_TO_DECIMAL(amount_raw, 18, 2) IS NULL THEN 'INVALID_AMOUNT'
    WHEN TRY_TO_BOOLEAN(is_fraud_raw) IS NULL THEN 'INVALID_IS_FRAUD'
    ELSE 'VALID'
  END AS validation_status
FROM raw_trans;



-- DYNAMIC TABLE MERCHANTS
-- cleaning and SCD TYPE 1 logic implementation
CREATE OR REPLACE DYNAMIC TABLE dt_merchants

-- merchant location data to bardziej dane statystyczne
-- but we need to have reasonably fresh data because of possible new merchants 
-- warehouse bedzie wybudzany troche czesciej, ale tabela merchantow jest raczej mala
-- wiec koszt nie wzrosnie znaczaco a spojnosc danych znacznie wzrosnie
-- TODO: zrobic TEST i porownac koszty z TARGET_LAG 15 min vs 1h
  TARGET_LAG = '15 minutes'
  WAREHOUSE = dynamic_wh
AS
SELECT
  merchant_raw AS merchant,
  TRY_TO_DECIMAL(merchant_lat_raw, 11, 8) AS merchant_lat,
  TRY_TO_DECIMAL(merchant_long_raw, 11, 8) AS merchant_long,
  meta_load_ts

FROM raw_merch
-- deduplikacja rekordow - SCD type 1
-- najpierw wybieramy najnowszy rekord dla danego merchant_raw i czasu wczytania
-- potem filtrujemy tylko ten rekord (qualify row_number = 1)
-- w ten sposob zawsze mamy najnowsze dane dla danego sprzedawcy
-- (ze wzgledu na charakter danych (tylko lokalizacja) nie stosujemy SCD type 2)
QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant_raw ORDER BY meta_load_ts DESC, meta_file_row_number DESC) = 1;



-- DYNAMIC TABLE CLIENTS
-- cleaning and SCD TYPE 2 logic implementation
CREATE OR REPLACE DYNAMIC TABLE dt_clients

-- dane klientow moze nie zmieniaja sie az tak czesto
-- i wiekszy lag (kilka/nascie minut) nie bylby problemem
-- ALE istotne sa tu tez dane o nowych klientach
-- 5 minutowy lag is better for new client -> immediate transactions scenario
-- jest to kompromis pomiedzy kosztem a spojnoscia danych
  TARGET_LAG = '5 minutes'
  WAREHOUSE = dynamic_wh
AS
WITH deduped_clients AS (
  SELECT
    card_num_raw AS card_num,
    first_name_raw AS first_name,
    last_name_raw AS last_name,
    gender_raw AS gender,
    TRY_TO_DATE(date_birth_raw) AS date_birth,
    job_raw AS job,
    street_raw AS street,
    city_raw AS city,
    state_raw AS state,

  -- valid_from to czas wczytania rekordu
    meta_load_ts AS valid_from,
    meta_load_ts -- zachowujemy tez oryginalna nazwe dla spojnosci zapytan kontrolnych
  FROM raw_clients

  -- deduplikacja
  -- najpierw wybieramy najnowszy rekord dla danego card_num i czasu wczytania
  -- potem filtrujemy tylko ten rekord (qualify row_number = 1)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY card_num_raw, meta_load_ts ORDER BY meta_file_row_number DESC) = 1
),

scd_clients AS (
  SELECT
    *,
    -- funkcja okna lead(valid_from) pobiera date loadu nastepnego rekordu 
    -- i ustawia ja jako date konca (valid_to) dla aktualnego rekordu
    -- -> ciagla linai czasu, gdzie koniec jednego stanu jest poczatkiem nastepnego
    -- dla najnowszego rekordu funkcja zwroci NULL, co oznacza, ze jest to aktualny rekord
    -- dziala to TYLKO jesli para card_num i valid_from jest unikalna w tabeli
    -- dlatego pracujemy na CTE deduped_clients
    LEAD(valid_from) OVER (
      PARTITION BY card_num ORDER BY valid_from) AS valid_to,

    -- flaga is_current oznaczajaca czy rekord jest aktualny czy historyczny
    CASE WHEN LEAD(valid_from) OVER (
      PARTITION BY card_num ORDER BY valid_from) IS NULL THEN TRUE 
      ELSE FALSE 
    END AS is_current

  FROM deduped_clients
)
SELECT * FROM scd_clients;



-- DYNAMIC TABLE - INVALID TRANSACTIONS
-- to catch invalid transactions for further analysis
-- dynamic table fraud full bedzie miala tylko valid transactions
-- a nie chce calkowicie stracic informacji o invalid transactions
CREATE OR REPLACE DYNAMIC TABLE dt_invalid_trans
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = dynamic_wh
AS
SELECT 
  trans_num,
  validation_status,
  meta_load_ts,
  trans_ts,
  card_num,
  amount,
  merchant,
  category,
  is_fraud
FROM dt_transactions
WHERE validation_status != 'VALID';



--- DYNAMIC TABLE FRAUD FULL - ENRICHED DATASET ---
-- join transactions with clients and merchants
-- with scd logic applied and quick validation filter
CREATE OR REPLACE DYNAMIC TABLE dt_fraud_full

-- downstream to najlepsza opcja dla tabeli koncowej
-- tabela odswieza sie zawsze wtedy, gdy odswieza sie KTORAKOLWIEK z tabel zrodlowych
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = dynamic_wh
AS
SELECT
  -- transaction Data
  t.trans_num,
  t.trans_ts,
  t.card_num,
  t.amount,
  t.category,
  t.is_fraud,

  -- client data (historical state at moment of transaction)
  c.first_name,
  c.last_name,
  c.gender,
  c.job,
  c.city AS client_city,
  c.state AS client_state,

  -- merchant data)
  m.merchant,
  m.merchant_lat,
  m.merchant_long,
  t.meta_load_ts -- lineage: kiedy transakcja wpadla do systemu

FROM dt_transactions t

-- join with merchants (left join, bo mozna miec transakcje u nieznanego sprzedawcy)
LEFT JOIN dt_merchants m 
  ON t.merchant = m.merchant
  
-- join with clients
LEFT JOIN dt_clients c 
  ON t.card_num = c.card_num

  -- warunki point-in-time
  -- wybieramy rekord klienta, ktory byl wazny w momencie transakcji 
  AND t.trans_ts >= c.valid_from 
  AND (t.trans_ts < c.valid_to OR c.valid_to IS NULL)

-- filter only valid transactions
WHERE t.validation_status = 'VALID';


-- quick check data landed
SELECT COUNT(*) FROM dt_transactions;
SELECT COUNT(*) FROM dt_merchants;
SELECT COUNT(*) FROM dt_clients;
SELECT COUNT(*) FROM dt_invalid_transactions;
SELECT COUNT(*) FROM dt_fraud_full;
SELECT COUNT(*) FROM dt_invalid_transactions;

SELECT * FROM dt_transactions
ORDER BY meta_load_ts DESC LIMIT 10;
SELECT * FROM dt_merchants
ORDER BY meta_load_ts DESC LIMIT 10;
SELECT * FROM dt_clients
ORDER BY meta_load_ts DESC LIMIT 10;
SELECT * FROM dt_fraud_full
ORDER BY meta_load_ts DESC LIMIT 10;  