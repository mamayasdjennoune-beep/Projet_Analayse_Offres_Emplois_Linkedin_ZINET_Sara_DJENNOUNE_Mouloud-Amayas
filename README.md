# Projet-Analyse-des-Offres-d-Emploi-LinkedIn-avec-Snowflake
Analyse de bout en bout du marché de l’emploi LinkedIn à l’aide de Snowflake (architecture Bronze/Silver/Gold) et d’un tableau de bord interactif Streamlit.
## Introduction 

Ce projet a pour objectif d’analyser le marché de l’emploi LinkedIn à partir de données hétérogènes (CSV et JSON) stockées dans un bucket S3 public. Il s’inscrit dans une démarche complète de Data Engineering, allant de l’ingestion brute des données jusqu’à leur visualisation à travers un dashboard interactif. 

L’enjeu principal est double : 

Mettre en place un pipeline de données robuste et traçable dans Snowflake, basé sur l’architecture Bronze / Silver / Gold. 

Fournir des analyses exploitables via SQL et Streamlit afin de mieux comprendre les dynamiques du marché de l’emploi :
types de postes, niveaux de salaire, secteurs d’activité, tailles d’entreprises et compétences recherchées. 

Ce rapport détaille pas à pas chaque étape, explique chaque script SQL, justifie les choix techniques, et revient en profondeur sur les problèmes rencontrés et leurs solutions. 

Le script commence par la création d’une base de données nommée LINKEDIN, suivie du schéma BRONZE. Cette étape est fondamentale, car elle initialise l’espace de travail dans lequel toutes les données brutes seront déposées.
L’utilisation de IF NOT EXISTS garantit que la création est idempotente : le script peut être relancé plusieurs fois sans créer de doublons ou générer d’erreurs

```sql
-- Create Databse
CREATE  DATABASE IF NOT EXISTS  linkedin;
```
 
