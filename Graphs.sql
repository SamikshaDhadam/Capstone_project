CREATE OR REPLACE VIEW CUSTOMER360_AGGREGATE AS
SELECT
    COUNTRY,
    COUNT(DISTINCT CUSTOMER_ID) AS TOTAL_CUSTOMERS,
    AVG(TOTAL_BALANCE) AS AVG_BALANCE,
    SUM(TOTAL_SPENT) AS TOTAL_SPENT,
    COUNT(TOTAL_TRANSACTIONS) AS TOTAL_TRANSACTIONS,
    MAX(LAST_TRANSACTION_DATE) AS LAST_TRANSACTION_DATE,
    COUNT(DISTINCT CASE WHEN GENDER = 'Male' THEN CUSTOMER_ID END) AS MALE_CUSTOMERS,
    COUNT(DISTINCT CASE WHEN GENDER = 'Female' THEN CUSTOMER_ID END) AS FEMALE_CUSTOMERS,
    COUNT(DISTINCT CASE WHEN INCOME_BRACKET = 'High' THEN CUSTOMER_ID END) AS HIGH_INCOME_CUSTOMERS,
    COUNT(DISTINCT CASE WHEN INCOME_BRACKET = 'Medium' THEN CUSTOMER_ID END) AS MEDIUM_INCOME_CUSTOMERS,
    COUNT(DISTINCT CASE WHEN INCOME_BRACKET = 'Low' THEN CUSTOMER_ID END) AS LOW_INCOME_CUSTOMERS
FROM CUSTOMER360
GROUP BY COUNTRY;

select * from CUSTOMER360_AGGREGATE;


CREATE OR REPLACE VIEW FRAUD_MONITORING_VIEW AS
SELECT
    CUSTOMER_ID,
    TOTAL_SPENT,
    TOTAL_TRANSACTIONS,
    LAST_TRANSACTION_DATE,
    CASE
        WHEN TOTAL_SPENT > 10000 THEN 'High Risk'
        WHEN TOTAL_SPENT > 5000 AND TOTAL_SPENT <= 10000 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS RISK_LEVEL
FROM CUSTOMER360
ORDER BY TOTAL_SPENT DESC;


select * from FRAUD_MONITORING_VIEW;


SELECT MIN(TOTAL_SPENT) AS MIN_SPENT, MAX(TOTAL_SPENT) AS MAX_SPENT
FROM CUSTOMER360;

SELECT 
    RISK_LEVEL, 
    COUNT(*) AS CUSTOMER_COUNT 
FROM FRAUD_MONITORING_VIEW 
GROUP BY RISK_LEVEL 
ORDER BY CUSTOMER_COUNT DESC;


CREATE OR REPLACE VIEW FRAUD_MONITORING_VIEW AS
SELECT
    CUSTOMER_ID,
    TOTAL_SPENT,
    TOTAL_TRANSACTIONS,
    LAST_TRANSACTION_DATE,
    CASE
        WHEN TOTAL_SPENT > 10000 THEN 'High Risk'
        WHEN TOTAL_SPENT > 2000 AND TOTAL_SPENT <= 10000 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS RISK_LEVEL
FROM CUSTOMER360
ORDER BY TOTAL_SPENT DESC;


SELECT 
    MIN(TOTAL_SPENT) AS MIN_SPENT,
    MAX(TOTAL_SPENT) AS MAX_SPENT,
    COUNT(CASE WHEN TOTAL_SPENT > 10000 THEN 1 END) AS HIGH_RISK_COUNT,
    COUNT(CASE WHEN TOTAL_SPENT > 5000 AND TOTAL_SPENT <= 10000 THEN 1 END) AS MEDIUM_RISK_COUNT,
    COUNT(CASE WHEN TOTAL_SPENT <= 5000 THEN 1 END) AS LOW_RISK_COUNT
FROM CUSTOMER360;


