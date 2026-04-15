
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
## II. 5.	Création des tables Silver
```sql
-- Create Schema SILVER
CREATE SCHEMA IF NOT EXISTS LINKEDIN.SILVER;
```
La création du schéma `Silver` suis le même logique que le schéma `Bronze

`
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
- La requête SELECT * FROM LINKEDIN.SILVER.JOB_POSTINGS permet de vérifier le résultat final.

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
- La requête SELECT * FROM LINKEDIN.SILVER.BENEFITS permet de vérifier le résultat final.
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
- La requête SELECT * FROM LINKEDIN.SILVER.EMPLOYEE_COUNTS permet de vérifier le résultat.
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
- La table JOB_SKILLS est créée dans la couche Silver avec CREATE OR REPLACE TABLE.
- Les données proviennent de la table LINKEDIN.BRONZE.JOB_SKILLS.
- L’identifiant de l’offre est converti en numérique avec TRY_TO_NUMBER(job_id).
- Cette conversion permet d’utiliser job_id pour les jointures analytiques.
- Le champ skill_abr est nettoyé avec TRIM(skill_abr).
- La fonction NULLIF(..., '') remplace les chaînes vides par NULL.
- Aucune transformation métier complexe n’est appliquée à ce champ.
- Les compétences sont conservées sous forme abrégée.
- La table Silver est reconstruite à partir de la couche Bronze.
- Elle est prête à être utilisée dans la couche Gold.
- La requête SELECT * FROM LINKEDIN.SILVER.JOB_SKILLS permet de vérifier le contenu
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
- La table JOB_INDUSTRIES est créée dans la couche Silver avec CREATE OR REPLACE TABLE.
- Les données proviennent de la table LINKEDIN.BRONZE.JOB_INDUSTRIES.
- La fonction LATERAL FLATTEN(input => data) est utilisée pour parcourir le fichier JSON.
- Chaque élément du tableau JSON est transformé en une ligne.
- L’identifiant de l’offre est extrait avec f.value:job_id::BIGINT.
- Ce champ est converti en type numérique.
- L’identifiant du secteur est extrait avec f.value:industry_id::INT.
- Ce champ est également typé en entier.
- Aucune transformation métier n’est appliquée à ces valeurs.
- La table permet d’associer chaque offre à un secteur d’activité.
- Elle facilite les analyses par industrie dans les couches ultérieures.
- Les données sont structurées à partir de données semi‑structurées.
- La table Silver est entièrement reconstruite depuis la couche Bronze.
- La requête SELECT * FROM LINKEDIN.SILVER.JOB_INDUSTRIES permet de vérifier le résultat.

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
- La table COMPANY_INDUSTRIES est créée dans la couche Silver avec CREATE OR REPLACE TABLE.
- Les données proviennent de la table LINKEDIN.BRONZE.COMPANY_INDUSTRIES.
- La fonction LATERAL FLATTEN(input => data) est utilisée pour parcourir le JSON.
- Chaque association entreprise–industrie devient une ligne.
- L’identifiant de l’entreprise est extrait avec f.value:company_id::BIGINT.
- Ce champ est converti en type numérique.
- Le secteur d’activité est extrait avec f.value:industry::STRING.
- Le champ industry est nettoyé avec TRIM.
- La fonction NULLIF(..., '') remplace les chaînes vides par NULL.
- Aucune normalisation métier n’est appliquée au secteur.
- La table permet d’associer chaque entreprise à son industrie.
- Les données semi‑structurées sont transformées en format relationnel.
- La table Silver est reconstruite à partir de la couche Bronze.
- Elle est prête pour les jointures analytiques dans la couche Gold.
- La requête SELECT * FROM LINKEDIN.SILVER.COMPANY_INDUSTRIES permet de vérifier le résultat.

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
- La table COMPANY_SPECIALITIES est créée dans la couche Silver avec CREATE OR REPLACE TABLE.
- Les données proviennent de la table LINKEDIN.BRONZE.COMPANY_SPECIALITIES.
- La fonction LATERAL FLATTEN(input => data) est utilisée pour parcourir le fichier JSON.
- Chaque spécialité est transformée en une ligne distincte.
- L’identifiant de l’entreprise est extrait avec f.value:company_id::BIGINT.
- Ce champ est converti en type numérique.
- La spécialité est extraite avec f.value:speciality::STRING.
- Le champ speciality est nettoyé avec TRIM.
- La fonction NULLIF(..., '') remplace les chaînes vides par NULL.
- Aucune traduction automatique n’est appliquée aux spécialités.
- La table permet d’associer chaque entreprise à ses domaines d’expertise.
- Les données semi‑structurées sont converties en format relationnel.
- La table Silver est reconstruite à partir de la couche Bronze.
- Elle est prête pour les jointures dans la couche Gold.
- La requête SELECT * FROM LINKEDIN.SILVER.COMPANY_SPECIALITIES permet de vérifier le résultat.

 

## II. 6. 	Création des tables Gold
```sql
-- Create schema GOLD
CREATE SCHEMA IF NOT EXISTS LINKEDIN.GOLD;
```
* La création du shéma `Gold` suis la même logique que les schémas `Bronze` et `Silver`
 * Table `JOB_POSTING`
```sql
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


```
 * Table `JOB_INDUSTRIES`
```sql
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
```
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
  
 * Table `JOB_SKILLS`
```sql
	CREATE OR REPLACE TABLE LINKEDIN.GOLD.JOB_SKILLS AS
SELECT
    job_id,
    skill_abr
FROM LINKEDIN.SILVER.JOB_SKILLS
WHERE job_id IS NOT NULL
  AND skill_abr IS NOT NULL;

-- Check table content
SELECT * FROM LINKEDIN.GOLD.JOB_SKILLS;
```
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
  
* Table `COMPANY_PROFILE`
```sql
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

```
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

* Table `JOB_ANALYTICS`
```sql
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
* Table
```sql
	
```
* Table
```sql
	
```
## II. .8	Application streamlit
(Explication détaillée du code)
# III.	Difficultés et solutions apportées 
# IV.	Conclusions 



