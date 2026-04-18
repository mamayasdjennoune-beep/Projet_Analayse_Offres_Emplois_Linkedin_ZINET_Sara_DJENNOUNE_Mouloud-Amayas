-- Travail effectué par :  ZINET Sara, DJENNOUNE Mouloud-Amayas

-- Create Database
CREATE DATABASE IF NOT EXISTS LINKEDIN;

-- Create Schema BRONZE
CREATE SCHEMA IF NOT EXISTS LINKEDIN.BRONZE;

-- Create Stage
CREATE OR REPLACE STAGE LINKEDIN.BRONZE.LINKEDIN_STAGE
    URL = 's3://snowflake-lab-bucket/';

-- Create Table JOB_POSTINGS
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.JOB_POSTINGS (
    job_id STRING,
    company_name STRING,
    title STRING,
    description STRING,
    max_salary STRING,
    med_salary STRING,
    min_salary STRING,
    pay_period STRING,
    formatted_work_type STRING,
    location STRING,
    applies STRING,
    original_listed_time STRING,
    remote_allowed STRING,
    views STRING,
    job_posting_url STRING,
    application_url STRING,
    application_type STRING,
    expiry STRING,
    closed_time STRING,
    formatted_experience_level STRING,
    skills_desc STRING,
    listed_time STRING,
    posting_domain STRING,
    sponsored STRING,
    work_type STRING,
    currency STRING,
    compensation_type STRING
);

-- Copy data into JOB_POSTINGS
COPY INTO LINKEDIN.BRONZE.JOB_POSTINGS
FROM @LINKEDIN.BRONZE.LINKEDIN_STAGE/job_postings.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
);

-- Check table content
SELECT * FROM LINKEDIN.BRONZE.JOB_POSTINGS;


-- Create table BENEFITS
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.BENEFITS (
    job_id STRING,
    inferred STRING,
    type STRING
);

-- Copy data into BENEFITS
COPY INTO LINKEDIN.BRONZE.BENEFITS
FROM @LINKEDIN.BRONZE.LINKEDIN_STAGE/benefits.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
);

-- Check table content
SELECT * FROM LINKEDIN.BRONZE.BENEFITS;

-- Create table EMPLOYEE_COUNTS
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.EMPLOYEE_COUNTS (
    company_id STRING,
    employee_count STRING,
    follower_count STRING,
    time_recorded STRING
);

-- Copy data into EMPLOYEE_COUNTS
COPY INTO LINKEDIN.BRONZE.EMPLOYEE_COUNTS
FROM @LINKEDIN.BRONZE.LINKEDIN_STAGE/employee_counts.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
);

-- Check table content
SELECT * FROM LINKEDIN.BRONZE.EMPLOYEE_COUNTS;

-- Create table JOB_SKILLS
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.JOB_SKILLS (
    job_id STRING,
    skill_abr STRING
);

-- Copy data into JOB_SKILLS
COPY INTO LINKEDIN.BRONZE.JOB_SKILLS
FROM @LINKEDIN.BRONZE.LINKEDIN_STAGE/job_skills.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
);

-- Check table content
SELECT * FROM LINKEDIN.BRONZE.JOB_SKILLS;

-- Create table COMPANIES
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.COMPANIES (
    data VARIANT
);

-- Copy data into COMPANIES
COPY INTO LINKEDIN.BRONZE.COMPANIES
FROM @LINKEDIN.BRONZE.LINKEDIN_STAGE/companies.json
FILE_FORMAT = (
    TYPE = 'JSON'
);

-- Check table content
SELECT * FROM LINKEDIN.BRONZE.COMPANIES;

-- Create table JOB_INDUSTRIES
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.JOB_INDUSTRIES (
    data VARIANT
);

-- Copy data into JOB_INDUSTRIES
COPY INTO LINKEDIN.BRONZE.JOB_INDUSTRIES
FROM @LINKEDIN.BRONZE.LINKEDIN_STAGE/job_industries.json
FILE_FORMAT = (
    TYPE = 'JSON'
);

-- Check table content
SELECT * FROM LINKEDIN.BRONZE.JOB_INDUSTRIES;

-- Create table COMPANY_SPECIALITIES
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.COMPANY_SPECIALITIES (data VARIANT);
-- Copy the data into table
COPY INTO LINKEDIN.BRONZE.COMPANY_SPECIALITIES
FROM @linkedin_stage/company_specialities.json
FILE_FORMAT = (TYPE='JSON');

-- Check table content
select * from LINKEDIN.BRONZE.COMPANY_SPECIALITIES;

-- Create table COMPANY_INDUSTRIES
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.COMPANY_INDUSTRIES (data VARIANT);

-- Copy the data into table
COPY INTO LINKEDIN.BRONZE.COMPANY_INDUSTRIES
FROM @linkedin_stage/company_industries.json
FILE_FORMAT = (TYPE='JSON');

-- Check table content
select * from LINKEDIN.BRONZE.COMPANY_INDUSTRIES;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Create Schema SILVER
CREATE SCHEMA IF NOT EXISTS LINKEDIN.SILVER;

-- Create table JOB_POSTINGS
CREATE OR REPLACE TABLE LINKEDIN.SILVER.JOB_POSTINGS AS
SELECT
    job_id::BIGINT AS job_id,

    NULLIF(TRIM(company_name), '') AS company_name,
    NULLIF(TRIM(title), '') AS title,
    description AS description,

    TRY_TO_NUMBER(NULLIF(TRIM(max_salary), '')) AS max_salary,
    TRY_TO_NUMBER(NULLIF(TRIM(med_salary), '')) AS med_salary,
    TRY_TO_NUMBER(NULLIF(TRIM(min_salary), '')) AS min_salary,

    /* PAY PERIOD normalisé FR/EN */
    CASE
        WHEN LOWER(TRIM(pay_period)) IN ('per year','par an','annuel') THEN 'yearly'
        WHEN LOWER(TRIM(pay_period)) IN ('per month','par mois','mensuel') THEN 'monthly'
        WHEN LOWER(TRIM(pay_period)) IN ('per week','par semaine','hebdomadaire') THEN 'weekly'
        WHEN LOWER(TRIM(pay_period)) IN ('per day','par jour','journalier') THEN 'daily'
        WHEN LOWER(TRIM(pay_period)) IN ('per hour','par heure','horaire') THEN 'hourly'
        ELSE NULLIF(TRIM(pay_period), '')
    END AS pay_period,

    /* formatted_work_type normalisé */
    CASE
        WHEN LOWER(TRIM(formatted_work_type)) IN ('full-time','temps plein') THEN 'Full-time'
        WHEN LOWER(TRIM(formatted_work_type)) IN ('part-time','temps partiel') THEN 'Part-time'
        WHEN LOWER(TRIM(formatted_work_type)) IN ('contract','contrat') THEN 'Contract'
        WHEN LOWER(TRIM(formatted_work_type)) IN ('internship','stage') THEN 'Internship'
        WHEN LOWER(TRIM(formatted_work_type)) IN ('temporary','intérim','temporaire') THEN 'Temporary'
        WHEN LOWER(TRIM(formatted_work_type)) IN ('apprenticeship','apprentissage') THEN 'Apprenticeship'
        ELSE NULLIF(TRIM(formatted_work_type), '')
    END AS formatted_work_type,

    NULLIF(TRIM(location), '') AS location,

    TRY_TO_NUMBER(NULLIF(TRIM(applies), '')) AS applies,

    /* original_listed_time — timestamps en millisecondes */
    CASE
        WHEN TRY_TO_NUMBER(original_listed_time) > 100000000000 THEN 
            TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(original_listed_time) / 1000)
        ELSE 
            TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(original_listed_time))
    END AS original_listed_time,

    /* remote_allowed — version robuste (booléen, entier, texte, variant) */
    CASE
        WHEN TRY_TO_BOOLEAN(remote_allowed) IS NOT NULL
            THEN TRY_TO_BOOLEAN(remote_allowed)

        WHEN TRY_TO_NUMBER(remote_allowed) = 1 THEN TRUE
        WHEN TRY_TO_NUMBER(remote_allowed) = 0 THEN FALSE

        WHEN LOWER(TO_VARCHAR(remote_allowed)) IN ('true','yes','vrai','oui') THEN TRUE
        WHEN LOWER(TO_VARCHAR(remote_allowed)) IN ('false','no','faux','non') THEN FALSE

        ELSE NULL
    END AS remote_allowed,

    TRY_TO_NUMBER(NULLIF(TRIM(views), '')) AS views,

    job_posting_url,
    application_url,
    application_type,

    /* expiry */
    CASE
        WHEN TRY_TO_NUMBER(expiry) > 100000000000 THEN 
            TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(expiry) / 1000)
        ELSE 
            TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(expiry))
    END AS expiry,

    /* closed_time */
    CASE
        WHEN TRY_TO_NUMBER(closed_time) > 100000000000 THEN 
            TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(closed_time) / 1000)
        ELSE 
            TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(closed_time))
    END AS closed_time,

    /* formatted_experience_level normalisé */
    CASE
        WHEN LOWER(TRIM(formatted_experience_level)) IN ('internship','stage') THEN 'Internship'
        WHEN LOWER(TRIM(formatted_experience_level)) IN ('entry level','débutant','junior') THEN 'Entry level'
        WHEN LOWER(TRIM(formatted_experience_level)) IN ('associate','confirmé') THEN 'Associate'
        WHEN LOWER(TRIM(formatted_experience_level)) IN ('mid-senior level','expérimenté') THEN 'Mid-Senior level'
        WHEN LOWER(TRIM(formatted_experience_level)) IN ('director','directeur') THEN 'Director'
        WHEN LOWER(TRIM(formatted_experience_level)) IN ('executive','cadre dirigeant') THEN 'Executive'
        ELSE NULLIF(TRIM(formatted_experience_level), '')
    END AS formatted_experience_level,

    skills_desc,

    /* listed_time */
    CASE
        WHEN TRY_TO_NUMBER(listed_time) > 100000000000 THEN 
            TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(listed_time) / 1000)
        ELSE 
            TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(listed_time))
    END AS listed_time,

    posting_domain,

    /* sponsored — version robuste */
    CASE
        WHEN TRY_TO_BOOLEAN(sponsored) IS NOT NULL
            THEN TRY_TO_BOOLEAN(sponsored)

        WHEN TRY_TO_NUMBER(sponsored) = 1 THEN TRUE
        WHEN TRY_TO_NUMBER(sponsored) = 0 THEN FALSE

        WHEN LOWER(TO_VARCHAR(sponsored)) IN ('true','yes','vrai','oui') THEN TRUE
        WHEN LOWER(TO_VARCHAR(sponsored)) IN ('false','no','faux','non') THEN FALSE

        ELSE NULL
    END AS sponsored,

    /* work_type normalisé */
    CASE
        WHEN LOWER(TRIM(work_type)) IN ('full-time','temps plein') THEN 'Full-time'
        WHEN LOWER(TRIM(work_type)) IN ('part-time','temps partiel') THEN 'Part-time'
        WHEN LOWER(TRIM(work_type)) IN ('contract','contrat') THEN 'Contract'
        WHEN LOWER(TRIM(work_type)) IN ('internship','stage') THEN 'Internship'
        WHEN LOWER(TRIM(work_type)) IN ('temporary','intérim','temporaire') THEN 'Temporary'
        ELSE NULLIF(TRIM(work_type), '')
    END AS work_type,

    currency,
    compensation_type

FROM LINKEDIN.BRONZE.JOB_POSTINGS

QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY job_id
        ORDER BY TRY_TO_TIMESTAMP(listed_time) DESC
    ) = 1;
-- Check table content
SELECT * FROM LINKEDIN.SILVER.JOB_POSTINGS;

-- Create table BENEFITS (SILVER)
CREATE OR REPLACE TABLE LINKEDIN.SILVER.BENEFITS AS
SELECT
    job_id::BIGINT AS job_id,

    -- Normalisation FR/EN des booléens
    CASE
        WHEN LOWER(TRIM(inferred)) IN ('true','1','yes','vrai','oui') THEN TRUE
        WHEN LOWER(TRIM(inferred)) IN ('false','0','no','faux','non') THEN FALSE
        ELSE NULL
    END AS inferred,

    -- Nettoyage du type (pas de traduction automatique possible)
    NULLIF(TRIM(type), '') AS type

FROM LINKEDIN.BRONZE.BENEFITS


QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY job_id, LOWER(TRIM(type))
        ORDER BY job_id
    ) = 1;
-- Check table content
SELECT * FROM LINKEDIN.SILVER.BENEFITS;


-- Create table COMPANIES
CREATE OR REPLACE TABLE LINKEDIN.SILVER.COMPANIES AS
SELECT
    f.value:company_id::BIGINT AS company_id,

    NULLIF(TRIM(f.value:name::STRING), '') AS name,
    f.value:description::STRING AS description,

    f.value:company_size::INT AS company_size,

    NULLIF(TRIM(f.value:state::STRING), '') AS state,
    NULLIF(TRIM(f.value:country::STRING), '') AS country,
    NULLIF(TRIM(f.value:city::STRING), '') AS city,
    NULLIF(TRIM(f.value:zip_code::STRING), '') AS zip_code,
    NULLIF(TRIM(f.value:address::STRING), '') AS address,
    NULLIF(TRIM(f.value:url::STRING), '') AS url

FROM LINKEDIN.BRONZE.COMPANIES,
     LATERAL FLATTEN(input => data) f;



-- Create table EMPLOYEE_COUNTS (SILVER)
;
CREATE OR REPLACE TABLE LINKEDIN.SILVER.EMPLOYEE_COUNTS AS
SELECT
    TRY_TO_NUMBER(company_id) AS company_id,
    TRY_TO_NUMBER(NULLIF(TRIM(employee_count), '')) AS employee_count,
    TRY_TO_NUMBER(NULLIF(TRIM(follower_count), '')) AS follower_count,
    CASE
        WHEN TRY_TO_NUMBER(time_recorded) > 100000000000
            THEN TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(time_recorded) / 1000)
        ELSE TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(time_recorded))
    END AS time_recorded
FROM LINKEDIN.BRONZE.EMPLOYEE_COUNTS
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY TRY_TO_NUMBER(company_id)
        ORDER BY TRY_TO_NUMBER(time_recorded) DESC
    ) = 1;
     -- Create table JOB_SKILLS
CREATE OR REPLACE TABLE LINKEDIN.SILVER.JOB_SKILLS AS
SELECT
    TRY_TO_NUMBER(job_id) AS job_id,
    NULLIF(UPPER(TRIM(skill_abr)), '') AS skill_abr
FROM LINKEDIN.BRONZE.JOB_SKILLS
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY TRY_TO_NUMBER(job_id), NULLIF(UPPER(TRIM(skill_abr)), '')
        ORDER BY job_id
    ) = 1;

-- Check table content
SELECT * FROM LINKEDIN.SILVER.JOB_SKILLS;
-- Create table JOB_INDUSTRIES
CREATE OR REPLACE TABLE LINKEDIN.SILVER.JOB_INDUSTRIES AS
SELECT
    job_id,
    industry_id
FROM (
    SELECT
        NULLIF(f.value:job_id::BIGINT, NULL) AS job_id,
        NULLIF(f.value:industry_id::INT, NULL) AS industry_id,
        ROW_NUMBER() OVER (
            PARTITION BY
                NULLIF(f.value:job_id::BIGINT, NULL),
                NULLIF(f.value:industry_id::INT, NULL)
            ORDER BY f.value:job_id
        ) AS rn
    FROM LINKEDIN.BRONZE.JOB_INDUSTRIES,
         LATERAL FLATTEN(input => data) f
)
WHERE rn = 1;

-- Check table content
SELECT * FROM JOB_INDUSTRIES;

--Create table COMPANY_INDUSTRIES

    -- Create table COMPANY_INDUSTRIES
CREATE OR REPLACE TABLE LINKEDIN.SILVER.COMPANY_INDUSTRIES AS
SELECT
    company_id,
    industry
FROM (
    SELECT
        f.value:company_id::BIGINT AS company_id,
        NULLIF(LOWER(TRIM(f.value:industry::STRING)), '') AS industry,
        ROW_NUMBER() OVER (
            PARTITION BY
                f.value:company_id::BIGINT,
                NULLIF(LOWER(TRIM(f.value:industry::STRING)), '')
            ORDER BY f.value:company_id
        ) AS rn
    FROM LINKEDIN.BRONZE.COMPANY_INDUSTRIES,
         LATERAL FLATTEN(input => data) f
)
WHERE rn = 1;
-- Check table content
select* from LINKEDIN.SILVER.COMPANY_INDUSTRIES;
-- Create table COMPANY_SPECIALITIES
CREATE OR REPLACE TABLE LINKEDIN.SILVER.COMPANY_SPECIALITIES AS
SELECT
    company_id,
    speciality
FROM (
    SELECT
        f.value:company_id::BIGINT AS company_id,
        NULLIF(LOWER(TRIM(f.value:speciality::STRING)), '') AS speciality,
        ROW_NUMBER() OVER (
            PARTITION BY
                f.value:company_id::BIGINT,
                NULLIF(LOWER(TRIM(f.value:speciality::STRING)), '')
            ORDER BY f.value:company_id
        ) AS rn
    FROM LINKEDIN.BRONZE.COMPANY_SPECIALITIES,
         LATERAL FLATTEN(input => data) f
)
WHERE rn = 1;
     
-- Check table content
select* from LINKEDIN.SILVER.COMPANY_SPECIALITIES;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Create schema GOLD
CREATE SCHEMA IF NOT EXISTS LINKEDIN.GOLD;

-- Create table JOB_POSTINGS (GOLD)
CREATE OR REPLACE TABLE LINKEDIN.GOLD.JOB_POSTINGS AS
SELECT
    job_id,
    company_name,
    title,
    description,
    max_salary,
    med_salary,
    min_salary,
    pay_period,
    formatted_work_type,
    location,
    applies,
    original_listed_time,
    remote_allowed,
    views,
    job_posting_url,
    application_url,
    application_type,
    expiry,
    closed_time,
    formatted_experience_level,
    skills_desc,
    listed_time,
    posting_domain,
    sponsored,
    work_type,
    currency,
    compensation_type
FROM LINKEDIN.SILVER.JOB_POSTINGS
WHERE job_id IS NOT NULL;

-- Check table content
SELECT * FROM LINKEDIN.GOLD.JOB_POSTINGS;

-- Create table JOB_INDUSTRIES
CREATE OR REPLACE TABLE LINKEDIN.GOLD.JOB_INDUSTRIES AS
SELECT
    job_id,
    industry_id
FROM LINKEDIN.SILVER.JOB_INDUSTRIES
WHERE job_id IS NOT NULL
  AND industry_id IS NOT NULL;
  
-- Check table content
select * from LINKEDIN.GOLD.JOB_INDUSTRIES;


-- Create table JOB_SKILLS 
CREATE OR REPLACE TABLE LINKEDIN.GOLD.JOB_SKILLS AS
SELECT
    job_id,
    skill_abr
FROM LINKEDIN.SILVER.JOB_SKILLS
WHERE job_id IS NOT NULL
  AND skill_abr IS NOT NULL;

-- Check table content
SELECT * FROM LINKEDIN.GOLD.JOB_SKILLS;


-- Create table COMPANY_PROFILE 
CREATE OR REPLACE TABLE LINKEDIN.GOLD.COMPANY_PROFILE AS
SELECT
    c.company_id,
    c.name,
    c.company_size,
    c.city,
    c.state,
    c.country,
    ec.employee_count,
    ec.follower_count
FROM LINKEDIN.SILVER.COMPANIES c
LEFT JOIN LINKEDIN.SILVER.EMPLOYEE_COUNTS ec
    ON c.company_id = ec.company_id
WHERE c.company_id IS NOT NULL;

-- Check table content
SELECT * FROM LINKEDIN.GOLD.COMPANY_PROFILE;


-- Create table JOB_ANALYTICS
CREATE OR REPLACE TABLE LINKEDIN.GOLD.JOB_ANALYTICS AS
SELECT
    jp.job_id,
    jp.title,

    -- VRAI nom d'entreprise, issu de COMPANY_PROFILE
    cp.name AS company_name,

    jp.formatted_work_type,
    jp.work_type,
    jp.remote_allowed,
    jp.formatted_experience_level,
    jp.min_salary,
    jp.med_salary,
    jp.max_salary,
    jp.currency,

    ji.industry_id,

    cp.company_size,
    cp.country,

    jp.listed_time
FROM LINKEDIN.GOLD.JOB_POSTINGS jp
LEFT JOIN LINKEDIN.GOLD.JOB_INDUSTRIES ji
    ON jp.job_id = ji.job_id
LEFT JOIN LINKEDIN.GOLD.COMPANY_PROFILE cp
    --  company_name dans JOB_POSTINGS = identifiant → on le convertit
    ON TRY_TO_NUMBER(jp.company_name) = cp.company_id;

-- Check table content
SELECT * FROM LINKEDIN.GOLD.JOB_ANALYTICS;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Analyse des données
--Comptage global des données
SELECT COUNT(*) AS total, COUNT(industry_id) AS non_null_industry
FROM LINKEDIN.GOLD.JOB_ANALYTICS;

SELECT COUNT(*) AS total, COUNT(company_size) AS non_null_company_size
FROM LINKEDIN.GOLD.JOB_ANALYTICS;

--Top 10 des titres par industrie
SELECT industry_id, title, COUNT(*) AS nb_job_postings
FROM LINKEDIN.GOLD.JOB_ANALYTICS
WHERE industry_id IS NOT NULL AND title IS NOT NULL
GROUP BY industry_id, title
QUALIFY ROW_NUMBER() OVER (PARTITION BY industry_id ORDER BY nb_job_postings DESC) <= 10
ORDER BY industry_id, nb_job_postings DESC;

--Top 10 des titres les mieux payés par industrie
SELECT industry_id, title, ROUND(AVG(max_salary), 0) AS avg_max_salary
FROM LINKEDIN.GOLD.JOB_ANALYTICS
WHERE industry_id IS NOT NULL AND max_salary IS NOT NULL
GROUP BY industry_id, title
QUALIFY ROW_NUMBER() OVER (PARTITION BY industry_id ORDER BY avg_max_salary DESC) <= 10
ORDER BY industry_id, avg_max_salary DESC;

--Répartition des offres par taille d’entreprise
SELECT company_size, COUNT(*) AS nb_job_postings
FROM LINKEDIN.GOLD.JOB_ANALYTICS
WHERE company_size IS NOT NULL
GROUP BY company_size
ORDER BY company_size;

--on compare avec la distribution réelle des entreprises
SELECT company_size, COUNT(*) AS nb_companies
FROM LINKEDIN.GOLD.COMPANY_PROFILE
WHERE company_size IS NOT NULL
GROUP BY company_size
ORDER BY company_size;

--Répartition des offres par industrie
SELECT industry_id, COUNT(*) AS nb_job_postings
FROM LINKEDIN.GOLD.JOB_ANALYTICS
WHERE industry_id IS NOT NULL
GROUP BY industry_id
ORDER BY nb_job_postings DESC
LIMIT 30;

--Répartition des offres par type de contrat
SELECT formatted_work_type, COUNT(*) AS nb_job_postings
FROM LINKEDIN.GOLD.JOB_ANALYTICS
WHERE formatted_work_type IS NOT NULL
GROUP BY formatted_work_type
ORDER BY nb_job_postings DESC;

--Top 10 des compétences les plus demandées*
SELECT skill_abr, COUNT(DISTINCT job_id) AS nb_job_postings
FROM LINKEDIN.GOLD.JOB_SKILLS
WHERE skill_abr IS NOT NULL
GROUP BY skill_abr
ORDER BY nb_job_postings DESC
LIMIT 10;







    



