
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

* Une meilleure traçabilité.
* Une séparation claire des responsabilités.
* Une optimisation des performances analytiques.

# II . Étapes Réalisées
## II . 1. Création de la Base de Données

```sql
-- Create Databse
CREATE  DATABASE IF NOT EXISTS  linkedin;

```
La création d’une base de données dédiée au projet permet de centraliser l’ensemble des couches de l’architecture Medallion (Bronze, Silver et Gold), tandis que l’utilisation de la clause IF NOT EXISTS évite les erreurs lors des relances successives du script.
  ## II 2.2. Création du schéma Bronze 
```sql
-- Create Schema BRONZE
CREATE SCHEMA IF NOT EXISTS linkedin.BRONZE;

```
La couche Bronze a pour rôle de stocker les données brutes telles qu’elles sont reçues, sans appliquer de transformation métier, afin de garantir la traçabilité et l’intégrité des données sources tout au long du pipeline de traitement.
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
### Code
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

```
### Explications
Pour chaque type de fichier source au format csv (job postings, benefits, job skills, employee counts), une table est créée dans la couche BRONZE avec toutes les colonnes définies en STRING.
Ce choix est volontaire et s’inscrit pleinement dans la philosophie de la couche BRONZE : stocker la donnée telle qu’elle existe à la source, sans appliquer de transformation, sans typage et sans prise de décision métier.
Cette approche garantit :

* Une ingestion fiable et robuste.
* La conservation intégrale des données sources.
* La résilience du pipeline face aux irrégularités (formats incohérents, valeurs manquantes, chaînes vides, types mélangés).


Chargement des fichiers CSV avec COPY INTO
Une fois les tables BRONZE créées, la commande COPY INTO est utilisée pour importer les données depuis le stage S3 vers Snowflake.
Plusieurs options sont spécifiées afin de sécuriser l’ingestion :

*`SKIP_HEADER = 1` : permet d’ignorer la première ligne des fichiers CSV, qui contient les noms des colonnes.
*`FIELD_OPTIONALLY_ENCLOSED_BY = '"' `: garantit une lecture correcte des champs contenant des virgules ou des retours à la ligne, notamment dans les descriptions de postes.
* `TYPE = 'CSV'` : précise explicitement le format du fichier source.
Cette logique est identique pour l’ensemble des fichiers dont la source est au format CSV.

Les fichiers au format JSON (companies, job industries, company industries, company specialities) sont également ingérés dans la couche BRONZE.
Contrairement aux fichiers CSV, ces derniers sont stockés dans une table contenant une unique colonne de type `VARIANT`
Le type VARIANT est spécifique à Snowflake et permet de stocker des données semi‑structurées, telles que le JSON, sans imposer de schéma relationnel strict.
Dans le cadre de ce projet, les fichiers JSON sont : souvent imbriqués, organisés sous forme de tableaux, non directement exploitables sous forme tabulaire.

Ce choix permet :

 * Conserver l’intégralité de la structure originale du fichier JSON,
* Eviter toute perte d’information,
* Repousser l’interprétation et la structuration des données à la couche Silver.
La commande COPY INTO est également utilisée pour charger les fichiers JSON, en précisant simplement un format différent.La logique de chargement reste identique à celle des CSV, la seule différence réside dans le format spécifié (JSON au lieu de CSV).



Après chaque chargement, une requête SELECT * est exécutée afin de vérifier immédiatement le contenu de la table BRONZE et s’assurer que les données ont été correctement ingérées


## II. 5.	Création des tables dans le schéma Silver

### Code
```sql
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
--Check table content
select* from LINKEDIN.SILVER.COMPANIES;


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
-- Check table content
SELECT * FROM LINKEDIN.SILVER.EMPLOYEE_COUNTS;

-- Create table JOB_SKILLS
CREATE OR REPLACE TABLE LINKEDIN.SILVER.JOB_SKILLS AS
SELECT
    TRY_TO_NUMBER(job_id) AS job_id,
    UPPER(TRIM(skill_abr)) AS skill_abr
FROM LINKEDIN.BRONZE.JOB_SKILLS
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY TRY_TO_NUMBER(job_id), UPPER(TRIM(skill_abr))
        ORDER BY job_id
    ) = 1;

-- Check table content
SELECT * FROM LINKEDIN.SILVER.JOB_SKILLS;

--Create table JOB_INDUSTRIES 
CREATE OR REPLACE TABLE LINKEDIN.SILVER.JOB_INDUSTRIES AS
SELECT
    job_id,
    industry_id
FROM (
    SELECT
        f.value:job_id::BIGINT AS job_id,
        f.value:industry_id::INT AS industry_id,
        ROW_NUMBER() OVER (
            PARTITION BY
                f.value:job_id::BIGINT,
                f.value:industry_id::INT
            ORDER BY f.value:job_id
        ) AS rn
    FROM LINKEDIN.BRONZE.JOB_INDUSTRIES,
         LATERAL FLATTEN(input => data) f
)
WHERE rn = 1;
--Create table COMPANY_INDUSTRIES

     CREATE OR REPLACE TABLE LINKEDIN.SILVER.COMPANY_INDUSTRIES AS
SELECT
    company_id,
    industry
FROM (
    SELECT
        f.value:company_id::BIGINT AS company_id,
        LOWER(TRIM(f.value:industry::STRING)) AS industry,
        ROW_NUMBER() OVER (
            PARTITION BY
                f.value:company_id::BIGINT,
                LOWER(TRIM(f.value:industry::STRING))
            ORDER BY f.value:company_id
        ) AS rn
    FROM LINKEDIN.BRONZE.COMPANY_INDUSTRIES,
         LATERAL FLATTEN(input => data) f
)
WHERE rn = 1;
-- Check table content
select* from LINKEDIN.SILVER.COMPANY_INDUSTRIES;

--Create table COMPANY_SPECIALITIES
CREATE OR REPLACE TABLE LINKEDIN.SILVER.COMPANY_SPECIALITIES AS
SELECT
    company_id,
    speciality
FROM (
    SELECT
        f.value:company_id::BIGINT AS company_id,
        LOWER(TRIM(f.value:speciality::STRING)) AS speciality,
        ROW_NUMBER() OVER (
            PARTITION BY
                f.value:company_id::BIGINT,
                LOWER(TRIM(f.value:speciality::STRING))
            ORDER BY f.value:company_id
        ) AS rn
    FROM LINKEDIN.BRONZE.COMPANY_SPECIALITIES,
         LATERAL FLATTEN(input => data) f
)
WHERE rn = 1;
     
-- Check table content
select* from LINKEDIN.SILVER.COMPANY_SPECIALITIES;

```
#### Schéma `Silver`
La création du schéma `Silver` suis le même logique que le schéma `Bronze, il a pour rôle de nettoyer, typer et normaliser les données issues de la couche Bronze en appliquant des règles de qualité et de cohérence, afin de préparer des données fiables et structurées pour l’analyse.
#### Table `JOB_POSTINGS`
- La table JOB_POSTINGS est créée dans la couche Silver avec l’instruction CREATE OR REPLACE TABLE.
- Les données proviennent directement de LINKEDIN.BRONZE.JOB_POSTINGS via la clause FROM.
- L’identifiant de l’offre est converti en numérique avec job_id::BIGINT.
- Les champs texte sont nettoyés grâce à TRIM(company_name) et NULLIF(..., '').
- Le titre du poste est également nettoyé avec NULLIF(TRIM(title), '').
- Les salaires sont convertis en nombres avec TRY_TO_NUMBER(max_salary), med_salary et min_salary.
- La fonction TRY_TO_NUMBER évite les erreurs de conversion.
- La période de paiement est normalisée à l’aide d’un CASE WHEN sur pay_period.
- Les valeurs françaises et anglaises sont regroupées sous des formats standards comme yearly ou monthly.
- Le type de contrat est harmonisé avec un CASE WHEN appliqué à formatted_work_type.
- Le champ work_type est normalisé selon la même logique.
- La localisation est nettoyée avec NULLIF(TRIM(location), '').
- Le nombre de candidatures est converti avec TRY_TO_NUMBER(applies).
- Le nombre de vues est converti avec TRY_TO_NUMBER(views).
- Les dates sont transformées avec TO_TIMESTAMP_NTZ.
- Une condition CASE WHEN permet de distinguer les secondes des millisecondes.
- Le champ remote_allowed est converti en booléen avec TRY_TO_BOOLEAN.
- Les valeurs numériques et textuelles sont aussi prises en compte dans le CASE.
- Le champ sponsored est normalisé selon la même logique booléenne.
- Le niveau d’expérience est standardisé grâce à un CASE WHEN sur formatted_experience_level.
- Les colonnes skills_desc, currency et compensation_type sont conservées sans transformation.
- La table Silver est ainsi reconstruite à partir de la table Bronze.
- La déduplication est réalisée via `ROW_NUMBER()` combiné à `QUALIFY`, en conservant uniquement l’offre la plus récente  par `job_id`.
- La requête SELECT * FROM LINKEDIN.SILVER.JOB_POSTINGS permet de vérifier le résultat final.

 #### Table `BENEFITS`

- La table BENEFITS est créée dans la couche Silver avec CREATE OR REPLACE TABLE.
- Elle est construite à partir de la table LINKEDIN.BRONZE.BENEFITS via la clause FROM.
- L’identifiant de l’offre est converti en numérique avec job_id::BIGINT.
- Cette conversion permet d’utiliser job_id comme clé de jointure.
- Le champ inferred est normalisé à l’aide d’un CASE WHEN.
- La fonction LOWER(TRIM(inferred)) permet d’unifier les valeurs textuelles.
- Les valeurs comme 'true', '1', 'yes', 'vrai' et 'oui' sont converties en TRUE.
- Les valeurs comme 'false', '0', 'no', 'faux' et 'non' sont converties en FALSE.
- Les valeurs non reconnues sont remplacées par NULL.
- Cette logique permet de gérer les formats français et anglais.
- Le champ type est nettoyé avec TRIM(type).
- La fonction NULLIF(..., '') transforme les chaînes vides en valeurs nulles.
- Aucune traduction automatique n’est appliquée au champ type.
- Les données sont ainsi standardisées sans perte d’information.
- La table Silver est entièrement reconstruite à partir de la couche Bronze.
-  La déduplication garantit une seule occurrence par couple `(job_id, type)`.
- La requête SELECT * FROM LINKEDIN.SILVER.BENEFITS permet de vérifier le résultat final.
  
 #### Table `COMPANIES`

- La table COMPANIES est créée dans la couche Silver avec CREATE OR REPLACE TABLE.
- Les données proviennent de la table LINKEDIN.BRONZE.COMPANIES.
- La fonction LATERAL FLATTEN(input => data) est utilisée pour lire le fichier JSON.
- Chaque objet JSON est transformé en une ligne relationnelle.
- L’identifiant de l’entreprise est extrait avec f.value:company_id::BIGINT.
- Ce champ devient la clé principale de la table.
- Le nom de l’entreprise est nettoyé avec TRIM et NULLIF.
- Les chaînes vides sont remplacées par des valeurs nulles.
- La description est extraite avec f.value:description::STRING.
- La taille de l’entreprise est convertie en entier avec company_size::INT.
- Les champs state, country et city sont extraits du JSON.
- Le code postal est récupéré avec f.value:zip_code::STRING.
- L’adresse complète est stockée dans le champ address.
- L’URL de l’entreprise est extraite avec f.value:url::STRING.
- Toutes les colonnes sont typées lors de l’extraction.
- Aucune transformation métier complexe n’est appliquée à ce stade.
- Cette table permet de structurer les données semi‑structurées.
- Elle prépare les données pour les jointures analytiques futures.
- La requête SELECT * FROM LINKEDIN.SILVER.COMPANIES permet de vérifier le résultat.
  
 #### Table `EMPLOYEE_COUNTS`

- La table EMPLOYEE_COUNTS est créée dans la couche Silver avec CREATE OR REPLACE TABLE.
- Les données proviennent de la table LINKEDIN.BRONZE.EMPLOYEE_COUNTS.
- L’identifiant de l’entreprise est converti en numérique avec TRY_TO_NUMBER(company_id).
- Cette conversion permet d’utiliser company_id pour les jointures.
- Le champ employee_count est nettoyé avec TRIM et NULLIF.
- Il est converti en nombre avec TRY_TO_NUMBER.
- Le champ follower_count suit la même logique de conversion.
- La fonction TRY_TO_NUMBER évite les erreurs de typage.
- Le champ time_recorded est converti en timestamp avec TO_TIMESTAMP_NTZ.
- Une condition CASE WHEN gère les formats en secondes et en millisecondes.
- Le seuil > 100000000000 permet d’identifier les millisecondes.
- Les valeurs nulles sont explicitement gérées dans le CASE.
- Cette logique garantit une cohérence temporelle des données.
- La table Silver est entièrement reconstruite à partir de la couche Bronze.
- Une déduplication explicite est appliquée afin de conserver uniquement l’enregistrement le plus récent par entreprise.
- La requête SELECT * FROM LINKEDIN.SILVER.EMPLOYEE_COUNTS permet de vérifier le résultat.
  
 #### Table `JOB_SKILLS`
 
- La table JOB_SKILLS est créée à partir de la couche Bronze.
- L’identifiant de l’offre est converti en numérique.
- Les compétences sont nettoyées et normalisées en majuscules afin d’éviter les doublons liés à la casse.
- Une déduplication est appliquée sur la clé métier (job_id, skill_abr).
- Cette approche garantit une seule occurrence de chaque compétence par offre.
- La requête SELECT * FROM LINKEDIN.SILVER.JOB_SKILLS permet de vérifier le contenu
 #### Table `JOB_INDUSTRIES`
- La table JOB_INDUSTRIES est créée dans la couche Silver avec CREATE OR REPLACE TABLE.
- Les données proviennent de la table LINKEDIN.BRONZE.JOB_INDUSTRIES.
- La fonction LATERAL FLATTEN(input => data) est utilisée pour parcourir le fichier JSON.
- Chaque élément du tableau JSON est transformé en une ligne.
- L’identifiant de l’offre est extrait avec f.value:job_id::BIGINT.
- Ce champ est converti en type numérique.
- L’identifiant du secteur est extrait avec f.value:industry_id::INT.
- Ce champ est également typé en entier.
- Une déduplication explicite est appliquée sur (job_id, industry_id) à l’aide de ROW_NUMBER().
- La table Silver est entièrement reconstruite depuis la couche Bronze.
- La requête SELECT * FROM LINKEDIN.SILVER.JOB_INDUSTRIES permet de vérifier le résultat.

### Table `COMPANY_INDUSTRIES`
- La table COMPANY_INDUSTRIES est créée dans la couche Silver avec CREATE OR REPLACE TABLE.
- Les données proviennent de la table LINKEDIN.BRONZE.COMPANY_INDUSTRIES.
- La fonction LATERAL FLATTEN(input => data) est utilisée pour parcourir le JSON.
-  L’identifiant de l’entreprise est extrait avec f.value:company_id::BIGINT.
-  Le secteur d’activité est extrait avec f.value:industry::STRING.
- Le champ industry est normalisé avec LOWER(TRIM(...)).
- Une déduplication est appliquée sur (company_id, industry).
- La table Silver est reconstruite à partir de la couche Bronze.
- La requête SELECT * FROM LINKEDIN.SILVER.COMPANY_INDUSTRIES permet de vérifier le résultat.

 #### Table `COMPANY_SPECIALITIES`
- La table COMPANY_SPECIALITIES est créée dans la couche Silver avec CREATE OR REPLACE TABLE.
- Les données proviennent de la table LINKEDIN.BRONZE.COMPANY_SPECIALITIES.
- La fonction LATERAL FLATTEN(input => data) est utilisée pour parcourir le fichier JSON.
- L’identifiant de l’entreprise est extrait avec f.value:company_id::BIGINT.
- La spécialité est extraite avec f.value:speciality::STRING.
- La casse et les espaces sont neutralisés via `LOWER(TRIM())`.
- Aucune traduction automatique n’est appliquée aux spécialités.
- La table permet d’associer chaque entreprise à ses domaines d’expertise.
- Les données semi‑structurées sont converties en format relationnel.
- La table Silver est reconstruite à partir de la couche Bronze.
- Elle est prête pour les jointures dans la couche Gold.
- La requête SELECT * FROM LINKEDIN.SILVER.COMPANY_SPECIALITIES permet de vérifier le résultat.

 

## II. 6. 	Création des tables dans le schéma Gold
### Code
```sql
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

```
#### Schéma `Gold`
 La création du schéma `Gold` suis la même logique que les schémas `Bronze` et `Silver`, il a pour rôle de consolider et d’enrichir les données nettoyées afin de produire des tables analytiques optimisées, directement exploitables pour les analyses métier et les outils de visualisation.
#### Table `JOB_POSTING`

- La table JOB_POSTINGS est créée dans la couche Gold avec CREATE OR REPLACE TABLE.
- Les données proviennent de la table LINKEDIN.SILVER.JOB_POSTINGS.
- Cette table correspond à la version finale des offres d’emploi.
- Toutes les colonnes nettoyées en Silver sont conservées.
- Aucune transformation supplémentaire n’est appliquée aux champs.
- Le filtrage WHERE job_id IS NOT NULL garantit l’intégrité des données.
- Ce filtre élimine les offres sans identifiant valide.
- Les champs salariaux sont déjà normalisés en amont.
- Les champs temporels sont déjà convertis en timestamps.
- Les champs booléens sont déjà standardisés.
- La table Gold est prête pour l’analyse métier.
- Elle sert de source principale pour les tableaux de bord.
- Elle est utilisée directement par l’application Streamlit.
- La requête SELECT * FROM LINKEDIN.GOLD.JOB_POSTINGS permet de vérifier le contenu final.
 #### Table `JOB_INDUSTRIES`

- La table JOB_INDUSTRIES est créée dans la couche Gold avec CREATE OR REPLACE TABLE.
- Les données proviennent de la table LINKEDIN.SILVER.JOB_INDUSTRIES.
- Cette table contient les associations entre offres et secteurs.
- Seules les colonnes job_id et industry_id sont sélectionnées.
- Le filtre WHERE job_id IS NOT NULL garantit un identifiant valide.
- Le filtre AND industry_id IS NOT NULL garantit un secteur valide.
- Ces filtres améliorent la qualité des données analytiques.
- Aucune transformation supplémentaire n’est appliquée aux champs.
- La table Gold est optimisée pour l’analyse par industrie.
- Elle est utilisée dans les requêtes analytiques finales.
- Elle est exploitée par l’application Streamlit.
- La requête SELECT * FROM LINKEDIN.GOLD.JOB_INDUSTRIES permet de vérifier le contenu.
  
 #### Table `JOB_SKILLS`

- La table JOB_SKILLS est créée dans la couche Gold avec CREATE OR REPLACE TABLE.
- Les données proviennent de la table LINKEDIN.SILVER.JOB_SKILLS.
- Seules les colonnes job_id et skill_abr sont sélectionnées.
- Le filtre WHERE job_id IS NOT NULL garantit un identifiant valide.
- Le filtre AND skill_abr IS NOT NULL garantit une compétence valide.
- Ces filtres améliorent la qualité des données analytiques.
- Aucune transformation supplémentaire n’est appliquée aux champs.
- La table Gold contient uniquement des associations exploitables.
- Elle permet d’analyser les compétences par offre d’emploi.
- Elle est utilisée dans les analyses finales et les tableaux de bord.
- La requête SELECT * FROM LINKEDIN.GOLD.JOB_SKILLS permet de vérifier le contenu.
  
#### Table `COMPANY_PROFILE`

- La table COMPANY_PROFILE est créée dans la couche Gold avec CREATE OR REPLACE TABLE.
- Les données proviennent de la table LINKEDIN.SILVER.COMPANIES, référencée par l’alias c.
- Les informations sur les effectifs proviennent de LINKEDIN.SILVER.EMPLOYEE_COUNTS, référencée par l’alias ec.
- Les deux tables sont reliées avec un LEFT JOIN.
- La jointure est effectuée sur c.company_id = ec.company_id.
- Le LEFT JOIN permet de conserver toutes les entreprises.
- Les entreprises sans données d’effectifs sont néanmoins conservées.
- Les champs company_id, name et company_size sont sélectionnés depuis la table COMPANIES.
- Les champs city, state et country décrivent la localisation de l’entreprise.
- Les champs employee_count et follower_count proviennent de la table EMPLOYEE_COUNTS.
- Le filtre WHERE c.company_id IS NOT NULL garantit un identifiant valide.
- Aucune transformation supplémentaire n’est appliquée aux champs.
- La table Gold regroupe les informations clés des entreprises.
- Elle sert de référence pour les analyses métier.
- Elle est utilisée dans les jointures avec les offres d’emploi.
- La requête SELECT * FROM LINKEDIN.GOLD.COMPANY_PROFILE permet de vérifier le contenu final.

#### Table `JOB_ANALYTICS`

- La table JOB_ANALYTICS est créée dans la couche Gold avec CREATE OR REPLACE TABLE.
- Elle constitue la table analytique centrale du projet.
- Les données principales proviennent de LINKEDIN.GOLD.JOB_POSTINGS, alias jp.
- La jointure avec LINKEDIN.GOLD.JOB_INDUSTRIES, alias ji, est réalisée avec un LEFT JOIN.
- Cette jointure est basée sur la condition jp.job_id = ji.job_id.
- Elle permet d’associer chaque offre à son secteur d’activité.
- Une seconde jointure est effectuée avec LINKEDIN.GOLD.COMPANY_PROFILE, alias cp.
- Cette jointure utilise TRY_TO_NUMBER(jp.company_name) = cp.company_id.
- Elle permet de récupérer le vrai nom de l’entreprise.
- Le champ cp.name est renommé en company_name.
- Les informations sur le type de contrat proviennent de formatted_work_type et work_type.
- Le champ remote_allowed indique la possibilité de télétravail.
- Le niveau d’expérience est récupéré via formatted_experience_level.
- Les champs min_salary, med_salary et max_salary sont conservés.
- La devise est indiquée par le champ currency.
- Le champ industry_id permet l’analyse par secteur.
- La taille de l’entreprise est récupérée via cp.company_size.
- Le pays de l’entreprise est fourni par cp.country.
- La date de publication de l’offre est stockée dans jp.listed_time.
- Les LEFT JOIN garantissent la conservation de toutes les offres.
- Les offres sans industrie ou sans entreprise restent présentes.
- La table Gold est optimisée pour les analyses métier.
- Elle est utilisée par les requêtes analytiques finales.
- Elle sert de source principale pour l’application Streamlit.
- La requête SELECT * FROM LINKEDIN.GOLD.JOB_ANALYTICS permet de vérifier le contenu.



## II. 7.	Requêtes d’analyse de données 
Après la construction des tables de la couche `Gold`, des requêtes d’analyse sont mises en place afin d’explorer et d’interpréter les données du marché de l’emploi. Ces requêtes permettent d’identifier les tendances clés liées aux offres d’emploi, aux secteurs d’activité, aux salaires et aux compétences demandées.
* Comptage global des offres et des industries
```sql
	--Comptage global des données
SELECT COUNT(*) AS total, COUNT(industry_id) AS non_null_industry
FROM LINKEDIN.GOLD.JOB_ANALYTICS;
```
Cette requête compte le nombre total d’offres d’emploi.  

Elle compte également le nombre d’offres associées à une industrie.  

La fonction COUNT(industry_id) ignore les valeurs nulles.  

Elle permet d’évaluer la couverture des données par secteur.  

* Comptage des entreprises avec taille connue
```sql
	SELECT COUNT(*) AS total, COUNT(company_size) AS non_null_company_size
FROM LINKEDIN.GOLD.JOB_ANALYTICS;
```
Cette requête compte le nombre total d’enregistrements analytiques.  

Elle compte les offres liées à une entreprise avec une taille renseignée.  

La comparaison permet d’identifier les données manquantes.  

*  Top 10 des titres de poste par industrie
```sql
	--Top 10 des titres par industrie
SELECT industry_id, title, COUNT(*) AS nb_job_postings
FROM LINKEDIN.GOLD.JOB_ANALYTICS
WHERE industry_id IS NOT NULL AND title IS NOT NULL
GROUP BY industry_id, title
QUALIFY ROW_NUMBER() OVER (PARTITION BY industry_id ORDER BY nb_job_postings DESC) <= 10
ORDER BY industry_id, nb_job_postings DESC;
```
Cette requête analyse les titres de poste les plus fréquents.  

Le regroupement est fait par industry_id et title.  

La fonction COUNT(*) calcule le nombre d’offres par titre.  

La fenêtre ROW_NUMBER() classe les titres par industrie.  

La clause QUALIFY limite le résultat aux 10 premiers titres.  

* Top 10 des postes les mieux payés par industrie
```sql
	--Top 10 des titres les mieux payés par industrie
SELECT industry_id, title, ROUND(AVG(max_salary), 0) AS avg_max_salary
FROM LINKEDIN.GOLD.JOB_ANALYTICS
WHERE industry_id IS NOT NULL AND max_salary IS NOT NULL
GROUP BY industry_id, title
QUALIFY ROW_NUMBER() OVER (PARTITION BY industry_id ORDER BY avg_max_salary DESC) <= 10
ORDER BY industry_id, avg_max_salary DESC;

```
Cette requête analyse les salaires maximums moyens.  

La fonction AVG(max_salary) calcule la moyenne des salaires.  

La fonction ROUND arrondit les valeurs pour une meilleure lisibilité.  

Le classement est effectué par industrie avec ROW_NUMBER().  

Seuls les 10 postes les mieux rémunérés par secteur sont conservés.  

* Répartition des offres par taille d’entreprise
```sql
	--Répartition des offres par taille d’entreprise
SELECT company_size, COUNT(*) AS nb_job_postings
FROM LINKEDIN.GOLD.JOB_ANALYTICS
WHERE company_size IS NOT NULL
GROUP BY company_size
ORDER BY company_size;

```
Cette requête analyse la distribution des offres par taille d’entreprise.  

Le regroupement est effectué sur company_size.  

La fonction COUNT(*) mesure le volume d’offres par catégorie.  

Le tri facilite la lecture des résultats.  

* Distribution réelle des entreprises par taille
```sql
	--on compare avec la distribution réelle des entreprises
SELECT company_size, COUNT(*) AS nb_companies
FROM LINKEDIN.GOLD.COMPANY_PROFILE
WHERE company_size IS NOT NULL
GROUP BY company_size
ORDER BY company_size;

```
Cette requête analyse la distribution réelle des entreprises.  

Elle s’appuie sur la table COMPANY_PROFILE.  

Elle permet de comparer offres publiées et structure du marché.  

* Répartition des offres par industrie
```sql
	--Répartition des offres par industrie
SELECT industry_id, COUNT(*) AS nb_job_postings
FROM LINKEDIN.GOLD.JOB_ANALYTICS
WHERE industry_id IS NOT NULL
GROUP BY industry_id
ORDER BY nb_job_postings DESC
LIMIT 30;
```
Cette requête identifie les industries les plus actives.  

Le nombre d’offres est calculé avec COUNT(*).  

Le tri décroissant met en évidence les secteurs dominants.  

La clause LIMIT 30 restreint l’analyse aux principaux secteurs.  

* Répartition des offres par type de contrat
```sql
	--Répartition des offres par type de contrat
SELECT formatted_work_type, COUNT(*) AS nb_job_postings
FROM LINKEDIN.GOLD.JOB_ANALYTICS
WHERE formatted_work_type IS NOT NULL
GROUP BY formatted_work_type
ORDER BY nb_job_postings DESC;

```
Cette requête analyse les types de contrats proposés.  
 
Le regroupement est effectué sur formatted_work_type.  

Elle permet d’identifier les formes d’emploi dominantes.  

* Top 10 des compétences les plus demandées
```sql
  --Top 10 des compétences les plus demandées
SELECT skill_abr, COUNT(DISTINCT job_id) AS nb_job_postings
FROM LINKEDIN.GOLD.JOB_SKILLS
WHERE skill_abr IS NOT NULL
GROUP BY skill_abr
ORDER BY nb_job_postings DESC
LIMIT 10;
 ```
Cette requête analyse les types de contrats proposés.  

Le regroupement est effectué sur formatted_work_type.  

Elle permet d’identifier les formes d’emploi dominantes.  

## II. .8	Application streamlit
L’application Streamlit constitue la couche finale de valorisation du projet. Elle permet de transformer les résultats issus de la couche Gold en visualisations interactives afin de faciliter l’analyse et l’interprétation du marché de l’emploi.
L’application s’appuie exclusivement sur les tables analytiques de la base LINKEDIN.GOLD, garantissant ainsi la cohérence entre les transformations effectuées en amont et les analyses présentées à l’utilisateur
*Le code complet de l'application :
```python
import streamlit as st
import pandas as pd
import altair as alt

st.set_page_config(layout="wide")

# ================================
# 🎨 STYLE GLOBAL
# ================================
st.markdown("""
    <style>
    .block-container {padding-top: 2rem;}
    h1, h2, h3 {font-weight: 600;}
    </style>
""", unsafe_allow_html=True)

st.title("💼 Job Market Insights")
st.caption("Real-time analytics powered by Snowflake")

# ================================
# 🔗 SNOWFLAKE SESSION
# ================================
from snowflake.snowpark.context import get_active_session
session = get_active_session()

# ================================
# 🔧 INDUSTRY MAPPING
# ================================
industry_map = {
    1: "Defense & Space", 3: "Computer Hardware", 4: "Computer Software",
    5: "Computer Networking", 6: "Internet", 7: "Computer & Network Security",
    8: "Information Technology & Services", 9: "Semiconductors",
    10: "Telecommunications", 11: "Law Practice", 12: "Legal Services",
    13: "Management Consulting", 14: "Biotechnology", 15: "Medical Practice",
    16: "Hospital & Health Care", 17: "Pharmaceuticals", 18: "Veterinary",
    19: "Medical Devices", 20: "Cosmetics", 21: "Apparel & Fashion",
    22: "Sporting Goods", 23: "Tobacco", 24: "Supermarkets",
    25: "Food Production", 26: "Consumer Electronics", 27: "Consumer Goods",
    28: "Furniture", 29: "Retail", 30: "Entertainment",
    31: "Gambling & Casinos", 32: "Leisure, Travel & Tourism",
    33: "Hospitality", 34: "Restaurants", 35: "Sports",
    36: "Food & Beverages", 37: "Motion Pictures & Film",
    38: "Broadcast Media", 39: "Museums & Institutions",
    40: "Fine Art", 41: "Performing Arts",
    42: "Recreational Facilities & Services", 43: "Banking",
    44: "Insurance", 45: "Financial Services", 46: "Real Estate",
    47: "Investment Banking", 48: "Investment Management",
    49: "Accounting", 50: "Construction", 51: "Building Materials",
    52: "Architecture & Planning", 53: "Civil Engineering",
    54: "Aviation & Aerospace", 55: "Automotive", 56: "Chemicals",
    57: "Machinery", 58: "Mining & Metals", 59: "Oil & Energy",
    60: "Utilities", 61: "Shipbuilding", 62: "Packaging & Containers",
    63: "Railroad Manufacture", 64: "Renewables & Environment",
    65: "Glass, Ceramics & Concrete", 66: "Textiles", 67: "Warehousing",
    68: "Airlines/Aviation", 69: "Maritime",
    70: "Transportation/Trucking/Railroad",
    71: "Logistics & Supply Chain", 72: "Import & Export",
    73: "Primary/Secondary Education", 74: "Higher Education",
    75: "Education Management", 76: "Research", 77: "Military",
    78: "Legislative Office", 79: "Judiciary",
    80: "International Affairs", 81: "Government Administration",
    82: "Executive Office", 83: "Law Enforcement",
    84: "Public Safety", 85: "Public Policy",
    86: "Marketing & Advertising", 87: "Newspapers",
    88: "Publishing", 89: "Printing", 90: "Information Services",
    91: "Libraries", 92: "Environmental Services",
    93: "Package/Freight Delivery", 94: "Individual & Family Services",
    95: "Religious Institutions", 96: "Civic & Social Organization",
    97: "Consumer Services", 98: "Nonprofit Organization Management",
    99: "Fund-Raising", 100: "Program Development",
    101: "Writing & Editing", 102: "Staffing & Recruiting",
    103: "Professional Training & Coaching", 104: "Market Research",
    105: "Public Relations & Communications", 106: "Design",
    107: "Graphic Design", 108: "Photography", 109: "Arts & Crafts",
    110: "Animation", 111: "Music", 112: "Online Media",
    113: "Events Services", 114: "Business Supplies & Equipment",
    115: "E-Learning", 116: "Outsourcing/Offshoring",
    117: "Facilities Services", 118: "Human Resources",
    119: "Venture Capital & Private Equity", 120: "Think Tanks",
    121: "Nanotechnology", 122: "Computer Games",
    123: "Alternative Medicine", 124: "Health, Wellness & Fitness",
    125: "Alternative Dispute Resolution", 126: "Mental Health Care",
    127: "Philanthropy", 128: "International Trade & Development",
    129: "Wireless", 130: "Capital Markets",
    131: "Political Organization", 132: "Translation & Localization",
    133: "Computer & Network Security", 134: "Farming",
    135: "Ranching", 136: "Dairy", 137: "Fishery",
    138: "Paper & Forest Products", 139: "Forestry",
    140: "Luxury Goods & Jewelry", 141: "Renewables & Environment",
    142: "Mechanical or Industrial Engineering",
    143: "Industrial Automation",
    144: "Electrical/Electronic Manufacturing",
    145: "Plastics", 146: "Rubber & Plastics",
    147: "Wholesale", 148: "Commercial Real Estate",
    149: "Banking", 150: "Leasing Real Estate"
}

def map_industry(df):
    df["industry_name"] = df["INDUSTRY_ID"].map(industry_map).fillna("Other")
    return df

# ================================
# 🎯 1. TOP ROLES
# ================================
st.markdown("## 🎯 Top 10 des titres de postes les plus publiés par industrie")

query1 = """
SELECT industry_id, title, COUNT(*) AS nb_jobs
FROM LINKEDIN.GOLD.JOB_ANALYTICS
WHERE industry_id IS NOT NULL AND title IS NOT NULL
GROUP BY industry_id, title
QUALIFY ROW_NUMBER() OVER (PARTITION BY industry_id ORDER BY nb_jobs DESC) <= 10
"""

df1 = map_industry(session.sql(query1).to_pandas())

industry = st.selectbox("Industry", sorted(df1["industry_name"].unique()))

df1 = (
    df1[df1["industry_name"] == industry]
    .sort_values("NB_JOBS", ascending=False)
    .head(10)
)

chart1 = alt.Chart(df1).mark_bar(size=18).encode(
    x=alt.X("NB_JOBS:Q", title="Job Openings"),
    y=alt.Y("TITLE:N", sort='-x'),
    color=alt.value("#4C78A8"),
    tooltip=["TITLE", "NB_JOBS"]
)

st.altair_chart(chart1, use_container_width=True)

# ================================
# 💰 2. SALARY
# ================================
st.markdown("## 💰 Top 10 des postes les mieux rémunérés par industrie")

query2 = """
SELECT industry_id, title, ROUND(AVG(max_salary),0) AS avg_salary
FROM LINKEDIN.GOLD.JOB_ANALYTICS
WHERE industry_id IS NOT NULL AND max_salary IS NOT NULL
GROUP BY industry_id, title
QUALIFY ROW_NUMBER() OVER (PARTITION BY industry_id ORDER BY avg_salary DESC) <= 10
"""

df2 = map_industry(session.sql(query2).to_pandas())

industry2 = st.selectbox("Industry (Salary)", sorted(df2["industry_name"].unique()))

df2 = (
    df2[df2["industry_name"] == industry2]
    .sort_values("AVG_SALARY", ascending=False)
    .head(10)
)

chart2 = alt.Chart(df2).mark_bar(size=18).encode(
    x=alt.X("AVG_SALARY:Q", title="Salary"),
    y=alt.Y("TITLE:N", sort='-x'),
    color=alt.Color("AVG_SALARY:Q", scale=alt.Scale(scheme="goldgreen")),
    tooltip=["TITLE", "AVG_SALARY"]
)

st.altair_chart(chart2, use_container_width=True)

# ================================
# 🏢 3. COMPANY SIZE DISTRIBUTION
# ================================
st.markdown("## 🏢 Répartition des offres d'emploi par taille d'entreprise")

query3 = """
SELECT company_size, COUNT(*) AS nb_jobs
FROM LINKEDIN.GOLD.JOB_ANALYTICS
WHERE company_size IS NOT NULL
GROUP BY company_size
ORDER BY company_size
"""

df3 = session.sql(query3).to_pandas()

chart3 = alt.Chart(df3).mark_line(point=True).encode(
    x=alt.X("COMPANY_SIZE:O", title="Company Size"),
    y=alt.Y("NB_JOBS:Q", title="Number of Jobs"),
    tooltip=["COMPANY_SIZE", "NB_JOBS"]
)

st.altair_chart(chart3, use_container_width=True)

top_size = df3.sort_values("NB_JOBS", ascending=False).iloc[0]
st.success(f"👉 Most opportunities come from company size: {top_size['COMPANY_SIZE']}")

# ================================
# 🌍 4. INDUSTRY HIRING (WAFFLE CHART PREMIUM)
# ================================
st.markdown("## 🌍 Hiring by Industry")

query4 = """
SELECT industry_id, COUNT(*) AS nb_jobs
FROM LINKEDIN.GOLD.JOB_ANALYTICS
WHERE industry_id IS NOT NULL
GROUP BY industry_id
ORDER BY nb_jobs DESC
LIMIT 10
"""

df4 = session.sql(query4).to_pandas()

# ================================
# 🎨 PREPARATION WAFFLE
# ================================
df4 = map_industry(df4)

total = df4["NB_JOBS"].sum()
df4["pct"] = df4["NB_JOBS"] / total

waffle = []
for i, row in df4.iterrows():
    blocks = int(row["pct"] * 100)
    waffle += [(row["industry_name"], 1)] * blocks

df_waffle = pd.DataFrame(waffle, columns=["industry", "value"])
df_waffle["id"] = range(len(df_waffle))

# ================================
# 📊 WAFFLE VISUAL (COULEURS CORRIGÉES)
# ================================
chart4 = alt.Chart(df_waffle).mark_square(size=120).encode(
    x=alt.X("id:Q", axis=None),
    y=alt.Y("value:Q", axis=None),
    color=alt.Color(
        "industry:N",
        scale=alt.Scale(
            domain=sorted(df_waffle["industry"].unique()),
            range=[
                "#2563eb",  # bleu
                "#10b981",  # vert
                "#f59e0b",  # orange
                "#ef4444",  # rouge
                "#8b5cf6",  # violet
                "#06b6d4",  # cyan
                "#84cc16",  # lime
                "#f97316",  # orange foncé
                "#ec4899",  # rose
                "#64748b"   # gris
            ]
        ),
        legend=alt.Legend(title="Industries")
    ),
    tooltip=["industry"]
).properties(height=200)

st.altair_chart(chart4, use_container_width=True)

# ================================
# 📌 INSIGHT BOX
# ================================
top_ind = df4.iloc[0]

st.info(f"""
🌍 Industrie dominante : **{top_ind['industry_name']}**  
📊 Volume : **{int(top_ind['NB_JOBS']):,} offres**
""")

# ================================
# 📌 5. EMPLOIS PAR TYPE (VERSION PREMIUM FR)
# ================================
st.markdown("## 📌 Répartition des types de contrats")

query5 = """
SELECT formatted_work_type, COUNT(*) AS nb_jobs
FROM LINKEDIN.GOLD.JOB_ANALYTICS
WHERE formatted_work_type IS NOT NULL
GROUP BY formatted_work_type
ORDER BY nb_jobs DESC
"""

df5 = session.sql(query5).to_pandas()

# ================================
# 🎨 STYLE KPI MODERNE (CARDS COLORÉES)
# ================================
st.markdown("""
<style>
.kpi-card {
    background: linear-gradient(135deg, #ffffff, #f8f9ff);
    padding: 18px;
    border-radius: 16px;
    box-shadow: 0 6px 18px rgba(0,0,0,0.06);
    text-align: center;
    border: 1px solid rgba(0,0,0,0.05);
}

.kpi-title {
    font-size: 13px;
    color: #6b7280;
    margin-bottom: 8px;
}

.kpi-value {
    font-size: 28px;
    font-weight: 700;
}
</style>
""", unsafe_allow_html=True)

# ================================
# 🔥 GRAPHIQUE (SEULEMENT COULEURS MODIFIÉES)
# ================================
chart5 = alt.Chart(df5).mark_bar(
    cornerRadiusTopLeft=6,
    cornerRadiusTopRight=6
).encode(
    x=alt.X("NB_JOBS:Q", title="Nombre d'emplois"),
    y=alt.Y(
        "FORMATTED_WORK_TYPE:N",
        sort='-x',
        title="Type de contrat"
    ),
    color=alt.Color(
        "FORMATTED_WORK_TYPE:N",
        scale=alt.Scale(
            domain=[
                "Full-time",
                "Contract",
                "Part-time",
                "Internship",
                "Temporary",
                "Volunteer"
            ],
            range=[
                "#2563eb",  # bleu
                "#f59e0b",  # orange
                "#10b981",  # vert
                "#8b5cf6",  # violet
                "#ef4444",  # rouge
                "#ec4899"   # rose
            ]
        ),
        legend=None
    ),
    tooltip=["FORMATTED_WORK_TYPE", "NB_JOBS"]
).properties(height=260)

st.altair_chart(chart5, use_container_width=True)

# ================================
# 💎 KPI CARDS (INCHANGÉ)
# ================================
cols = st.columns(len(df5))

for i, row in df5.iterrows():

    label = str(row["FORMATTED_WORK_TYPE"])
    value = int(row["NB_JOBS"])

    color = "#64748b"

    if "Full" in label:
        color = "#2563eb"
    elif "Contract" in label:
        color = "#f59e0b"
    elif "Part" in label:
        color = "#10b981"
    elif "Intern" in label:
        color = "#8b5cf6"
    elif "Temp" in label:
        color = "#ef4444"
    elif "Volunteer" in label:
        color = "#ec4899"

    cols[i].markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Type de contrat</div>
        <div class="kpi-value" style="color:{color};">
            {value:,}
        </div>
        <div style="font-size:13px; margin-top:5px; color:#111;">
            {label}
        </div>
    </div>
    """, unsafe_allow_html=True)
```
* Explication du code:
### 1. Configuration et initialisation de l’application
Le paramétrage de l’application est réalisé à l’aide de la fonction st.set_page_config(layout="wide"), permettant un affichage en pleine largeur adapté aux tableaux de bord analytiques.
Un style CSS personnalisé est injecté via st.markdown afin d’améliorer la lisibilité et l’esthétique générale de l’interface. Les titres et la mise en page sont ainsi homogénéisés pour offrir une expérience utilisateur plus professionnelle.

### 2. Connexion à Snowflake
L’application établit une connexion directe à Snowflake à l’aide de la fonction get_active_session. Cette approche permet d’exécuter des requêtes SQL directement depuis Streamlit, sans duplication des données côté application.
Toutes les données affichées dans l’application sont donc extraites en temps réel depuis la base de données Snowflake, assurant des analyses toujours à jour.

### 3. Mapping des industries LinkedIn
Un dictionnaire de correspondance (industry_map) est utilisé afin d’associer les identifiants numériques des industries LinkedIn à leurs libellés métiers officiels. Ce mapping améliore considérablement la lisibilité des résultats pour l’utilisateur.
La fonction map_industry applique ce mapping aux jeux de données retournés par les requêtes SQL. Une nouvelle colonne industry_name est ainsi ajoutée, permettant d’afficher des noms d’industries explicites plutôt que des codes numériques.
Cette étape assure une meilleure compréhension métier des analyses présentées dans l’application.

### 4. Analyse des titres de postes par industrie
La première visualisation permet d’identifier les 10 titres de postes les plus fréquemment publiés par industrie.
Les données sont extraites depuis la table LINKEDIN.GOLD.JOB_ANALYTICS à l’aide d’une requête SQL utilisant une fonction analytique ROW_NUMBER.
Un filtre interactif selectbox permet à l’utilisateur de sélectionner une industrie spécifique. Le graphique en barres généré avec Altair affiche alors les titres de postes les plus représentés dans le secteur choisi.
Cette analyse met en évidence les profils les plus demandés selon les domaines d’activité.

### 5. Analyse des salaires par industrie
Une seconde section de l’application analyse les postes les mieux rémunérés par industrie.
La requête SQL calcule le salaire maximum moyen à l’aide de la fonction AVG(max_salary).
Un filtre par industrie est également proposé afin de comparer les rémunérations selon le secteur d’activité. Le graphique permet ainsi d’identifier les postes offrant les salaires les plus élevés dans chaque industrie.
Cette analyse apporte une vision économique du marché de l’emploi.

### 6. Répartition des offres par taille d’entreprise
L’application présente ensuite la distribution des offres d’emploi en fonction de la taille des entreprises.
Les données sont regroupées par company_size et visualisées sous forme de graphique linéaire.
Un message d’insight automatique met en avant la taille d’entreprise générant le plus grand nombre d’opportunités, facilitant l’interprétation des résultats.

### 7. Analyse du recrutement par industrie
Une visualisation avancée de type waffle chart est utilisée pour représenter la répartition des recrutements par industrie.
Chaque carré représente une proportion du volume total d’offres, offrant une lecture rapide et visuelle de l’activité de recrutement par secteur.
Cette représentation met en évidence les industries dominantes du marché de l’emploi LinkedIn.

### 8. Répartition des types de contrats
L’application analyse également la répartition des types de contrats proposés sur le marché, tels que les contrats à temps plein, à temps partiel ou les stages.
Les résultats sont affichés sous forme de graphique en barres, complété par des cartes KPI colorées indiquant le nombre total d’offres pour chaque type de contrat.
Cette section permet d’identifier les formes d’emploi les plus répandues.

### 9. Apport global de l’application Streamlit
L’application Streamlit complète efficacement les traitements réalisés dans Snowflake en offrant une interface interactive et visuelle. Elle permet une exploration dynamique des données et rend les analyses accessibles à un public non technique.
L’ensemble des visualisations repose sur des requêtes SQL cohérentes avec celles décrites dans le rapport, assurant une continuité entre la phase de transformation des données et leur exploitation analytique.

# III.	Difficultés et solutions apportées 

# IV.	Conclusions 



