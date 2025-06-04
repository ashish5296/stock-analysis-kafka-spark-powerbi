-- Step 1: Create the database
CREATE DATABASE IF NOT EXISTS stocks;

-- Step 2: Use the database
USE stocks;

-- Step 3: Create raw sentiment table (no header in CSV)
DROP TABLE IF EXISTS sentiment_raw;

CREATE TABLE sentiment_raw (
    s_date STRING,
    avg_sentiment DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Step 4: Load all sentiment CSVs from HDFS
LOAD DATA INPATH '/BigDataStreaming/sentiment'
INTO TABLE sentiment_raw;

-- Step 5: Create cleaned sentiment table with extracted date
DROP TABLE IF EXISTS sentiment;

CREATE TABLE sentiment AS
SELECT 
    CAST(SUBSTR(s_date, 1, 10) AS DATE) AS date_n,
    avg_sentiment
FROM sentiment_raw;

-- Step 6: Create raw stocks table (no header in CSV)
DROP TABLE IF EXISTS stocks_raw;

CREATE TABLE stocks_raw (
    s_date STRING,
    symbol STRING,
    open DOUBLE,
    close DOUBLE,
    high DOUBLE,
    low DOUBLE,
    movement_class INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Step 7: Load all stock CSVs from HDFS
LOAD DATA INPATH '/BigDataStreaming/stocks'
INTO TABLE stocks_raw;

-- Step 8: Create cleaned stocks table with extracted date
DROP TABLE IF EXISTS stocks_s;

CREATE TABLE stocks_s AS
SELECT 
    CAST(SUBSTR(s_date, 1, 10) AS DATE) AS date_s,
    symbol,
    open,
    close,
    high,
    low,
    movement_class
FROM stocks_raw;

-- Step 9: Join sentiment and stock tables on date
DROP TABLE IF EXISTS sentiment_stocks;

CREATE TABLE sentiment_stocks AS
SELECT 
    s.date_n, 
    s.avg_sentiment,
    st.symbol, 
    st.open, 
    st.close, 
    st.high, 
    st.low, 
    st.movement_class
FROM sentiment s
JOIN stocks_s st
    ON s.date_n = st.date_s;

-- Step 10: Export result to Google Cloud Storage bucket (GCS)
INSERT OVERWRITE DIRECTORY 'gs://aa_25_stocks_data/sentiment_stocks'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM sentiment_stocks;
