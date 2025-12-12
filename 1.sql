create or replace warehouse COMPUTE_WH
  warehouse_size = 'XSMALL'
  auto_suspend = 60
  auto_resume = true;
use warehouse COMPUTE_WH;

create or replace database FEATURE_DB;
use database FEATURE_DB;


create or replace schema RAW;
create or replace schema FEATURES;
use schema RAW;
-- show schemas in database FEATURE_DB;
create table cTransaction(
tId integer,
cId integer,
tDate date,
amount number(10,2),
catagory string
);

INSERT INTO cTransaction (tId,cId,tDate, amount, catagory)
VALUES
    (1, 101, '2024-01-01', 1200.50, 'electronics'),
    (2, 101, '2024-01-15', 800.00, 'groceries'),
    (3, 102, '2024-01-20', 500.00, 'fashion'),
    (4, 102, '2024-02-10', 750.00, 'electronics'),
    (5, 103, '2024-02-15', 300.00, 'groceries'),
    (6, 101, '2024-02-18', 2500.00, 'fashion');

SELECT * FROM cTransaction;


CREATE TABLE raw.customers (
    customerId INT,
    customerName STRING,
    age INT,
    salary FLOAT,
    country STRING
);

INSERT INTO raw.customers (customerId, customerName, age, salary, country)
VALUES
    (1, 'John', 28, 52000, 'USA'),
    (2, 'Alice', 35, 67000, 'UK'),
    (3, 'Ravi', 22, 45000, 'India');


CREATE OR REPLACE TABLE raw.customerFeatures AS
SELECT
    customerId,
    customerName,
    age,
    country,
    salary,
    CASE 
        WHEN age < 25 THEN 'youth'
        WHEN age BETWEEN 25 AND 40 THEN 'adult'
        ELSE 'senior'
    END AS ageGroup
FROM raw.customers;

ALTER TABLE raw.customerFeatures
ADD COLUMN normSalary FLOAT;

UPDATE raw.customerFeatures
SET normSalary = (salary - (SELECT MIN(salary) FROM raw.customerFeatures)) /((SELECT MAX(salary) FROM raw.customerFeatures) -(SELECT MIN(salary) FROM raw.customerFeatures));

ALTER TABLE raw.customerFeatures
  ADD COLUMN salaryBucket STRING;

UPDATE raw.customerFeatures
SET salaryBucket = 
    CASE
        WHEN salary < 30000 THEN 'LOW'
        WHEN salary < 60000 THEN 'MEDIUM'
        ELSE 'HIGH'
    END;
CREATE SCHEMA IF NOT EXISTS featureStore;
CREATE TABLE featureStore.customerFeaturesFs AS
SELECT 
    customerId,
    customerName,
    age,
    salary,
    normSalary,
    salaryBucket
FROM raw.customerFeatures;
USE DATABASE FEATURE_DB;
USE SCHEMA featureStore;


SHOW DATABASES;
USE DATABASE FEATURE_DB;
SHOW SCHEMAS;
USE SCHEMA FEATURESTORE;
SHOW TABLES;

SELECT 
    customerid,
    SNOWFLAKE.CORTEX.REGRESSION_LINEAR(
        INPUT => OBJECT_CONSTRUCT(
            'age', age,
            'income', income,
            'salary', salary
        ),
        TARGET => normsalary
    ) AS predicted_normsalary
FROM FEATURE_DB.FEATURESTORE.CUSTOMERFEATURESFS;


SELECT 
    CUSTOMERID,
    SNOWFLAKE.CORTEX.REGRESSION_LINEAR(
        INPUT => OBJECT_CONSTRUCT(
            'age', AGE,
            'salary', SALARY
        ),
        TARGET => NORMSALARY
    ) AS predicted_normSalary
FROM FEATURE_DB.FEATURESTORE.CUSTOMERFEATURESFS;


SHOW FUNCTIONS LIKE 'SNOWFLAKE.CORTEX%';
WITH stats AS (
  SELECT
    COUNT(*) AS n,
    AVG(age) AS avg_age,
    AVG(normSalary) AS avg_normSalary,
    SUM(age * normSalary) AS sum_xy,
    SUM(age * age) AS sum_x2
  FROM FEATURE_DB.FEATURESTORE.CUSTOMERFEATURESFS
)
SELECT 
  (sum_xy - n * avg_age * avg_normSalary) /
  (sum_x2 - n * avg_age * avg_age) AS slope
FROM stats;


WITH stats AS (
  SELECT
    AVG(age) AS avg_age,
    AVG(normSalary) AS avg_normSalary,
    (
      SELECT 
        (SUM(age * normSalary) - COUNT(*) * AVG(age) * AVG(normSalary)) /
        (SUM(age * age) - COUNT(*) * AVG(age) * AVG(age))
      FROM FEATURE_DB.FEATURESTORE.CUSTOMERFEATURESFS
    ) AS slope
  FROM FEATURE_DB.FEATURESTORE.CUSTOMERFEATURESFS
)
SELECT avg_normSalary - (slope * avg_age) AS intercept
FROM stats;


SET slope = 0.07748742648;
SET intercept = -1.756083118;

SELECT
  customerId,
  age,
  normSalary,
  (:intercept + :slope * age) AS predictedNormSalary
FROM FEATURE_DB.FEATURESTORE.CUSTOMERFEATURESFS;


INSERT INTO CUSTOMERS
VALUES
(4, 'Asutosh', 22, 25000, 'India'),
(5, 'parija', 25, 40000, 'Jermany');

SELECT * FROM CUSTOMERS;



INSERT INTO CUSTOMERS
VALUES
(6, 'asish', 55, 75000, 'India');

CREATE OR REPLACE TABLE RAW.CUSTOMERFEATURES AS 
SELECT 
    customerId,
    customerName,
    age,
    COUNTRY,
    SALARY, 
    CASE 
        WHEN AGE<25 THEN 'young'
        WHEN AGE BETWEEN 25 AND 40 THEN 'adult'
        ELSE 'senior'
    END AS ageGroup
FROM RAW.CUSTOMERS;

-- NOW IM TRYING T0 NORMALIZE THE SALARY AND ADD THEM AS THR NORMSALARY 
ALTER TABLE raw.customerFeatures ADD COLUMN normSalary FLOAT;

UPDATE raw.customerFeatures
SET NORMSALARY = (salary - (SELECT MIN(salary) FROM raw.customerFeatures)) /
                   ((SELECT MAX(salary) FROM raw.customerFeatures) - (SELECT MIN(salary) FROM raw.customerFeatures));
SELECT * FROM customerfeatures;


-- Adding the salary bucket 
ALTER TABLE raw.customerFeatures
ADD COLUMN salaryBucket STRING;


UPDATE raw.customerFeatures
SET salaryBucket = 
    CASE
        WHEN salary < 30000 THEN 'LOW'
        WHEN salary < 60000 THEN 'MEDIUM'
        ELSE 'HIGH'
    END;
SELECT * FROM customerfeaturesfs;

-- Creating the Feature Store Schema 

USE SCHEMA FEATURESTORE;
CREATE OR REPLACE TABLE featureStore.customerFeaturesFs AS
SELECT
    customerId,
    customerName,
    age,
    salary,
    normSalary,
    ageGroup,
    salaryBucket
FROM raw.customerFeatures;

-- Makeing large and synthetic data to make the ML model more realistic 
INSERT INTO raw.customers (customerId, customerName, age, salary, country)
SELECT
    seq4() + 56 AS customerId, 
    'Customer_' || seq4() AS customerName,
    UNIFORM(20, 60, RANDOM())::INT AS age,
    UNIFORM(30000, 100000, RANDOM()) AS salary, 
    CASE UNIFORM(1,4,RANDOM()) 
        WHEN 1 THEN 'USA'
        WHEN 2 THEN 'UK'
        WHEN 3 THEN 'India'
        ELSE 'Germany'
    END AS country
FROM TABLE(GENERATOR(ROWCOUNT => 50));
DELETE FROM raw.customers WHERE customerId > 99;


-- Modifying the customerFeature table
CREATE OR REPLACE TABLE raw.customerFeatures AS
SELECT
    customerId,
    customerName,
    age,
    country,
    salary,
    CASE 
        WHEN age < 25 THEN 'youth'
        WHEN age BETWEEN 25 AND 40 THEN 'adult'
        ELSE 'senior'
    END AS ageGroup,
    (salary - MIN(salary) OVER()) / (MAX(salary) OVER() - MIN(salary) OVER()) AS normSalary,
    CASE
        WHEN salary < 50000 THEN 'LOW'
        WHEN salary < 75000 THEN 'MEDIUM'
        ELSE 'HIGH'
    END AS salaryBucket
FROM raw.customers;
SELECT * FROM CUSTOMERFEATURES;

-- Modifying the customerFeatureCF table

CREATE OR REPLACE TABLE featureStore.customerFeaturesFs AS
SELECT
    customerId,
    customerName,
    age,
    salary,
    normSalary,
    ageGroup,
    salaryBucket
FROM raw.customerFeatures;
USE SCHEMA FEATURESTORE;

SELECT * FROM CUSTOMERFEATURESFS;


-- We are using simple Linear Regression using age → normSalary
WITH stats AS (
  SELECT
    AVG(age) AS avg_age,
    AVG(normSalary) AS avg_normSalary,
    SUM(age * normSalary) - COUNT(*) * AVG(age) * AVG(normSalary) AS numerator,
    SUM(age * age) - COUNT(*) * AVG(age) * AVG(age) AS denominator
  FROM featureStore.customerFeaturesFs
)
SELECT
  numerator / denominator AS slope,
  avg_normSalary - (numerator / denominator) * avg_age AS intercept
FROM stats;
--  Predicting normal salary 
SET slope = -0.001022215817;
SET intercept = 0.5838777207;
SELECT
  customerId,
  age,
  normSalary,
  ($intercept + $slope * age) AS predictedNormSalary
FROM featureStore.customerFeaturesFs;