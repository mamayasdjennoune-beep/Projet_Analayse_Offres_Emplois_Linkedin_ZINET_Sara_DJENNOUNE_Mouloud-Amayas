
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
### II.3. Configuration du Stage Externe

un stage Snowflake est configuré pour pointer vers un bucket S3 public. Ce stage joue le rôle d’un connecteur externe permettant à Snowflake d’accéder directement aux fichiers CSV et JSON stockés dans le cloud. Cette étape prépare donc l’ingestion des données provenant de LinkedIn.
```sql
-- Create Stage  
CREATE OR REPLACE STAGE LINKEDIN.BRONZE.linkedin_stage
URL = 's3://snowflake-lab-bucket/';

```
### 2.4 Création des tables et chargement des données
Pour chaque type de fichier (job postings, benefits, skills, employee counts…), une table est créée dans la couche BRONZE avec toutes les colonnes en STRING. Ce choix volontaire suit la philosophie de la couche BRONZE : stocker la donnée telle qu’elle existe, sans transformation, sans typage, sans prise de décision métier. Cela garantit une ingestion fiable, même si les fichiers contiennent des irrégularités.


La commande COPY INTO est ensuite utilisée pour importer les données depuis le stage S3 vers Snowflake. L’option SKIP_HEADER=1 permet d’éviter l’ingestion de la ligne d’en‑tête des CSV, tandis que l’option FIELD_OPTIONALLY_ENCLOSED_BY sécurise l’ingestion des champs contenant des guillemets. Après chaque chargement, une requête SELECT * assure une vérification instantanée du contenu de la table BRONZE. 


Les fichiers JSON sont eux aussi ingérés dans des tables BRONZE, mais contrairement aux CSV, ils sont stockés dans une unique colonne VARIANT. Cela permet de conserver la structure JSON originale, avec ses attributs imbriqués. Une conséquence directe est que chaque fichier JSON contenant un tableau est ingéré sous forme d’une seule ligne, ce qui nécessitera une correction en SILVER.


*  Table Benefits :


 
