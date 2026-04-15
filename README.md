
# I. Introduction 
## I. 1. Contexte
Avec l’essor des plateformes professionnelles comme LinkedIn, une grande quantité de données est générée quotidiennement autour du marché de l’emploi : offres d’emploi, compétences demandées, secteurs d’activité, types de contrats, salaires, etc.
Ces données représentent une source stratégique majeure pour analyser les tendances du recrutement, comprendre les besoins des entreprises et anticiper l’évolution des compétences recherchées.  

Dans ce projet, nous exploitons un jeu de données LinkedIn en mettant en œuvre une architecture data moderne de type Medallion (Bronze / Silver / Gold) sur Snowflake, couplée à une application de visualisation interactive développée avec Streamlit. 


## I. 2. Objectifs  
Les objectifs principaux du projet sont :

* Mettre en place une architecture de données robuste et scalable
* Nettoyer, normaliser et structurer des données hétérogènes
* Construire des tables analytiques optimisées
* Réaliser des requêtes d’analyse métiers
* Développer un tableau de bord interactif pour la visualisation des résultats

  
## I. 3. Présentation du jeu de données
Le projet repose sur plusieurs fichiers issus de LinkedIn :
|Fichier |                      Description  |
|--------|-----------------------------------|  
job_postings.csv  | Offres d’emploi (poste, salaire, localisation, type de contrat…) |
|benefits.csv               | Avantages liés aux offres  |
|job_skills.csv	                   |  Compétences associées aux offres |
|employee_counts.csv           |    Taille et popularité des entreprises   |
|companies.json               |  Informations détaillées sur les entreprises |
|job_industries.json               |  Secteurs d’activité des offres |
|company_industries.json	               |  Secteurs des entreprises |
|company_specialities.json	               | Spécialités des entreprises |

Ces données sont hétérogènes (CSV + JSON), avec des formats variables et parfois bruités.
## I. 4. Architecture Medallion
L’architecture Medallion se compose de trois couches :

* `Bronze` : données brutes, sans transformation
* `Silver` : données nettoyées, typées et normalisées
* `Gold` : données enrichies et prêtes pour l’analyse métier

Cette approche permet :

* une meilleure traçabilité,
* une séparation claire des responsabilités,
* une optimisation des performances analytiques.

# II . Étapes Réalisées
## II . 1. Création de la Base de Données

```sql
-- Create Databse
CREATE  DATABASE IF NOT EXISTS  linkedin;

```
* Crée une base de données dédiée au projet
* Centralise toutes les couches (Bronze, Silver, Gold)
* IF NOT EXISTS évite les erreurs lors des relances du script
  ## II 2.2. Création du schéma Bronze 
```sql
-- Create Schema BRONZE
CREATE SCHEMA IF NOT EXISTS linkedin.BRONZE;

```
Rôle du schéma Bronze :
*  Stockage des données brutes
* Aucune transformation métier
* Reproductibilité et auditabilité des données sources
## II.3. Configuration du Stage Externe

un stage Snowflake est configuré pour pointer vers un bucket S3 public. Ce stage joue le rôle d’un connecteur externe permettant à Snowflake d’accéder directement aux fichiers CSV et JSON stockés dans le cloud. Cette étape prépare donc l’ingestion des données provenant de LinkedIn.
```sql
-- Create Stage  
CREATE OR REPLACE STAGE LINKEDIN.BRONZE.linkedin_stage
URL = 's3://snowflake-lab-bucket/';

```
* Le stage Snowflake permet de faire le lien avec un stockage externe (S3)
* Centralise tous les fichiers sources
* Facilite les commandes COPY INTO
  
## 2.4 Création des tables dans le shéma Bronze et chargement des données
Pour chaque type de fichier (job postings, benefits, skills, employee counts…), une table est créée dans la couche BRONZE avec toutes les colonnes en STRING. Ce choix volontaire suit la philosophie de la couche BRONZE : stocker la donnée telle qu’elle existe, sans transformation, sans typage, sans prise de décision métier. Cela garantit une ingestion fiable, même si les fichiers contiennent des irrégularités.


La commande COPY INTO est ensuite utilisée pour importer les données depuis le stage S3 vers Snowflake. L’option SKIP_HEADER=1 permet d’éviter l’ingestion de la ligne d’en‑tête des CSV, tandis que l’option FIELD_OPTIONALLY_ENCLOSED_BY sécurise l’ingestion des champs contenant des guillemets. Après chaque chargement, une requête SELECT * assure une vérification instantanée du contenu de la table BRONZE. 


Les fichiers JSON sont eux aussi ingérés dans des tables BRONZE, mais contrairement aux CSV, ils sont stockés dans une unique colonne VARIANT. Cela permet de conserver la structure JSON originale, avec ses attributs imbriqués. Une conséquence directe est que chaque fichier JSON contenant un tableau est ingéré sous forme d’une seule ligne, ce qui nécessitera une correction en SILVER.
*  Table `Table JOB_POSTINGS` :
 ```sql
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

```
Cette instruction permet de créer la table `JOB_POSTINGS`  dans le schéma LINKEDIN.BRONZE avec toutes les colonnes présentes dans le fihcier csv. Ainsi, toutes les colonnes sont volontairement définies avec le type STRING, même lorsque les données représentent des nombres, des dates ou des booléens.

La commande `COPY INTO` permet de charger les données du fichier job_postings.csv stocké dans le stage Snowflake vers la table JOB_POSTINGS.  

Fonctionnement :

* `@LINKEDIN.BRONZE.LINKEDIN_STAGE` : référence au stage externe (stockage S3)
* `bjob_postings.csv` : fichier source contenant les offres d’emploi LinkedIn
* `bCOPY INTO` : méthode optimisée Snowflake pour le chargement massif de données

Paramètres du format CSV

* `TYPE = 'CSV'` : Indique le format du fichier source
 
* `SKIP_HEADER` = 1 : Ignore la première ligne contenant les noms des colonnes

* `FIELD_OPTIONALLY_ENCLOSED_BY` = '"' : Indispensable pour les fichiers CSV où les descriptions de postes contiennent des virgules. Cela indique à Snowflake que tout ce qui est entre guillemets appartient à la même colonne.


Avantages de cette approche

* Chargement rapide et fiable
* Compatibilité avec les fichiers générés automatiquement
* Réduction des erreurs liées au parsing des chaînes de caractères

  Une rêquete SELECT est ajoutée à la fin afin de voir un apperçu de la table et de bien de vérifier que les données sont corrrectement chrgées.
  La même la logique est appliquée pour les autre tables dont le fichier source est un fichier csv

  
*  Table `Benefits` :
```sql
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
```
* Table `EMPLOYEE_COUNTS` 
```sql
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
```
* Table `JOB_SKILLS`   
```sql
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
```
* Table `COMPANIES`
 ```sql
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

```
Cette instruction permet de créer la table COMPANIES dans le schéma LINKEDIN.BRONZE dont le fichier source est sous format `JSON`.

Choix du type VARIANT
La colonne unique data est définie avec le type VARIANT, qui est un type spécifique à Snowflake permettant de stocker des données semi‑structurées telles que `JSON`.


Dans le cadre de ce projet, les données sources sont fournies au format JSON, souvent imbriqué et non strictement tabulaire.
Objectifs de ce choix :

* Conserver l’intégralité de la structure originale du fichier JSON
* Éviter toute perte d’information
* Repousser l’interprétation du schéma à la couche Silver, où les données seront structurées

Ce choix est cohérent avec la philosophie de la couche Bronze, qui vise à stocker les données sans transformation, telles qu’elles sont reçues.

La commande La commande `COPY INTO` suis le méme raisonement que pour les fichier csv, la seule est différence est les format spécifié est le format : `JSON` 

 La même la logique est appliquée pour les autre tables dont le fichier source est un fichier JSON

* Table `JOB_INDUSTRIES
  `
```sql
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

```
* Table `COMPANY_SPECIALITIES`
 ```sql
   -- Create table COMPANY_SPECIALITIES
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.COMPANY_SPECIALITIES (data VARIANT);
-- Copy the data into table
COPY INTO LINKEDIN.BRONZE.COMPANY_SPECIALITIES
FROM @linkedin_stage/company_specialities.json
FILE_FORMAT = (TYPE='JSON');

-- Check table content
select * from LINKEDIN.BRONZE.COMPANY_SPECIALITIES;

```
* Table `COMPANY_INDUSTRIES`
 ```sql
   -- Create table COMPANY_INDUSTRIES
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.COMPANY_INDUSTRIES (data VARIANT);

-- Copy the data into table
COPY INTO LINKEDIN.BRONZE.COMPANY_INDUSTRIES
FROM @linkedin_stage/company_industries.json
FILE_FORMAT = (TYPE='JSON');

-- Check table content
select * from LINKEDIN.BRONZE.COMPANY_INDUSTRIES;

```
## II. 5.	Création des tables Siver
* Table `JOB_POSTINGS`
```sql
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

FROM LINKEDIN.BRONZE.JOB_POSTINGS;

-- Check table content
SELECT * FROM LINKEDIN.SILVER.JOB_POSTINGS;

```
 * Table `BENEFITS`
```sql
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

FROM LINKEDIN.BRONZE.BENEFITS;

-- 
SELECT * FROM LINKEDIN.SILVER.BENEFITS;


```
 * Table `COMPANIES`
```sql
	--Create table COMPANIES
CREATE OR REPLACE TABLE LINKEDIN.SILVER.COMPANIES AS
SELECT
    f.value:company_id::BIGINT  AS company_id,
    NULLIF(TRIM(f.value:name::STRING), '') AS name,
    f.value:description::STRING AS description,
    f.value:company_size::INT   AS company_size,
    f.value:state::STRING       AS state,
    f.value:country::STRING     AS country,
    f.value:city::STRING        AS city,
    f.value:zip_code::STRING    AS zip_code,
    f.value:address::STRING     AS address,
    f.value:url::STRING         AS url
FROM LINKEDIN.BRONZE.COMPANIES,
     LATERAL FLATTEN(input => data) f;
--Check table COMPANIES
select* from LINKEDIN.SILVER.COMPANIES;

```
 * Table `EMPLOYEE_COUNTS`
```sql
	-- Create table EMPLOYEE_COUNTS (SILVER)
CREATE OR REPLACE TABLE LINKEDIN.SILVER.EMPLOYEE_COUNTS AS
SELECT
    TRY_TO_NUMBER(company_id) AS company_id,

    TRY_TO_NUMBER(NULLIF(TRIM(employee_count), '')) AS employee_count,
    TRY_TO_NUMBER(NULLIF(TRIM(follower_count), '')) AS follower_count,

    -- Gestion seconds vs milliseconds
    CASE
        WHEN TRY_TO_NUMBER(time_recorded) IS NULL THEN NULL
        WHEN TRY_TO_NUMBER(time_recorded) > 100000000000  -- ~ 10^11 → probablement millisecondes
            THEN TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(time_recorded) / 1000)
        ELSE
            TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(time_recorded)) -- secondes
    END AS time_recorded

FROM LINKEDIN.BRONZE.EMPLOYEE_COUNTS;

-- Check table content
SELECT * FROM LINKEDIN.SILVER.EMPLOYEE_COUNTS;


```
 * Table `JOB_SKILLS`
```sql
	-- Create table JOB_SKILLS
CREATE OR REPLACE TABLE LINKEDIN.SILVER.JOB_SKILLS AS
SELECT
    TRY_TO_NUMBER(job_id) AS job_id,
    NULLIF(TRIM(skill_abr), '') AS skill_abr
FROM LINKEDIN.BRONZE.JOB_SKILLS;

-- Check table content
SELECT * FROM LINKEDIN.SILVER.JOB_SKILLS;
```
 * Table `JOB_INDUSTRIES`
```sql
	--Create table JOB_INDUSTRIES 
CREATE OR REPLACE TABLE LINKEDIN.SILVER.JOB_INDUSTRIES AS
SELECT
    f.value:job_id::BIGINT        AS job_id,
    f.value:industry_id::INT     AS industry_id
FROM LINKEDIN.BRONZE.JOB_INDUSTRIES,
     LATERAL FLATTEN(input => data) f;
--Check table JOB_INDUSTRIES 
select* from LINKEDIN.SILVER.JOB_INDUSTRIES;
```
* Table `COMPANY_INDUSTRIES`
```sql
--Create table COMPANY_INDUSTRIES
CREATE OR REPLACE TABLE LINKEDIN.SILVER.COMPANY_INDUSTRIES AS
SELECT
    f.value:company_id::BIGINT AS company_id,
    NULLIF(TRIM(f.value:industry::STRING), '') AS industry
FROM LINKEDIN.BRONZE.COMPANY_INDUSTRIES,
     LATERAL FLATTEN(input => data) f;
     
-- Check table content
select* from LINKEDIN.SILVER.COMPANY_INDUSTRIES;
```
 * Table `COMPANY_SPECIALITIES`
```sql
	--Create table COMPANY_SPECIALITIES
CREATE OR REPLACE TABLE LINKEDIN.SILVER.COMPANY_SPECIALITIES AS
SELECT
    f.value:company_id::BIGINT AS company_id,
    NULLIF(TRIM(f.value:speciality::STRING), '') AS speciality
FROM LINKEDIN.BRONZE.COMPANY_SPECIALITIES,
     LATERAL FLATTEN(input => data) f;
     
-- Check table content
select* from LINKEDIN.SILVER.COMPANY_SPECIALITIES;

 ```

## II. 6. 	Création des tables Gold
 * Table
	```sql
	
	```
 * Table
	```sql
	
	```
 * Table
	```sql
	
	```
## II. 7.	Requêtes d’analyse de données 
(Explication détaillée du code pour chaque requêtes)
## II. .8	Application streamlit
(Explication détaillée du code)
# III.	Difficultés et solutions apportées 
# IV.	Conclusions 



