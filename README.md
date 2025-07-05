# la doc officielle de mon projet weather dashboard 🦊
l'objectif de ce projet est de répondre à cette problèmatique :
> Peut-on recommander les meilleures périodes pour visiter une ville, selon des critères météo ?

🌟 Objectif

Créer une plateforme de visualisation de données météo permettant de :

Suivre des indicateurs météo-clés par ville et par période.

Filtrer dynamiquement par ville, mois, métrique.

Recommander les meilleures périodes pour visiter une ville selon la météo.

🛠 Outils Utilisés

Outil

Rôle

PostgreSQL

Stockage des données météo dans un modèle en étoile

Airflow

Orchestration des extractions, transformations et chargements

Open-Meteo API

Source des données météo (passé, présent, futur)

Metabase

Visualisation et exploration des données via des dashboards

Jupyter Notebook

Analyses exploratoires, prototypage rapide

📅 Étapes du Projet

1. 🛡️ Extraction des Données

API : https://api.open-meteo.com/v1/forecast et archive

Extraction pour plusieurs villes cibles : Bogota, Antananarivo, Taipei, etc.

Extraction en temps réel + prévisions (jusqu'à 7 jours).

2. 🔄 Transformation des Données

Calcul d'indicateurs agrégés (max température, précipitation totale, humidité moyenne, etc.).

Calcul d'un weather_score pour recommander les périodes idéales.

Agrégation mensuelle via city_weather_summary.

3. 🔢 Stockage (Modèle en étoile)

Fait principal : weather_fact

Dimensions : dim_city, dim_date

Table d'agrégation : city_weather_summary


Filtres dynamiques :

Ville

Année

Métrique

🔧 Fonctionnalités Clés

Historique météo consolidé

Prévisions pour les prochains jours

Système de score simple et pertinent pour le tourisme

Données exportables en JSON pour analyse supplémentaire

🔍 Limites & Prochaines Améliorations

Metabase : personnalisation limitée du design

Ajouter des prévisions long-terme

Automatiser l'envoi de recommandations
