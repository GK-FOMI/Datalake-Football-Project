import requests
import json
import os
import sys
from datetime import datetime, timedelta

def extract_player_stats(datalake_path):
    """
    Extrait les statistiques des joueurs via l'API-Football pour les données d'hier
    et les stocke dans la couche RAW du Datalake.
    """
    # --- 1. Configuration et Clés (REMPLACEZ VOTRE CLÉ) ---
    # Récupération de la clé API (idéalement via Airflow Variables ou un fichier .env, mais ici en dur pour l'exemple)
    API_KEY = "9e001ae9e38e3f2945c55ad706e7d687"
    BASE_URL = "https://v3.football.api-sports.io/" 
    
    HEADERS = {
        "X-RapidAPI-Key": API_KEY,
        "X-RapidAPI-Host": "api-football-beta.p.rapidapi.com"
    }

    # Logique d'extraction : On cherche les stats du jour précédent (J-1)
    # Le DAG s'exécute aujourd'hui, mais traite les données complètes de la journée passée.
    yesterday = datetime.now() - timedelta(days=1)
    date_str_raw = yesterday.strftime("%Y%m%d") # Format pour le chemin du dossier
    date_str_api = yesterday.strftime("%Y-%m-%d") # Format YYYY-MM-DD pour l'API

    # Paramètres de l'API (Exemple : Premier League ID 39, saison 2023)
    # Vous pouvez changer ces paramètres pour cibler la ligue et la saison souhaitées.
    PARAMS = {
        "league": "39",  
        "season": "2023", 
        #"date": "2025-12-15"  # Note: L'API-Football ne supporte pas le filtrage direct par date pour les joueurs
    }
    # --- Fin de Configuration ---

    # 2. Définition du chemin de stockage RAW
    # data/raw/api_football/players_stats/YYYYMMDD/
    raw_path = os.path.join(datalake_path, f"raw/api_football/players_stats/{date_str_raw}")
    
    try:
        os.makedirs(raw_path, exist_ok=True)
        filename = os.path.join(raw_path, f"stats_{datetime.now().hour}.json")

        print(f"Début de l'extraction des stats du {date_str_api} vers : {raw_path}")

        if API_KEY == "VOTRE_CLE_API_FOOTBALL_RAPIDAPI":
            raise ValueError("Erreur: Veuillez remplacer 'VOTRE_CLE_API_FOOTBALL_RAPIDAPI' par votre clé réelle.")

        # 3. Appel API
        response = requests.get(f"{BASE_URL}/players", headers=HEADERS, params=PARAMS)
        response.raise_for_status()  # Lève une exception si le statut n'est pas 2xx
        
        data = response.json()
        
        # 4. Sauvegarde dans le Datalake
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        
        print(f"Extraction réussie. Fichier enregistré: {filename}")
        print(f"Nombre de résultats bruts extraits: {data.get('results', 0)}")

    except requests.exceptions.RequestException as e:
        print(f"Erreur HTTP/Connexion (La clé API ou l'URL est probablement incorrecte) : {e}")
        sys.exit(1) # Échec de la tâche pour Airflow
    except ValueError as e:
        print(f"Erreur de configuration: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Point d'entrée pour le BashOperator Airflow
    if len(sys.argv) < 2:
        print("Erreur : Veuillez fournir le chemin racine du Datalake (/opt/airflow/data).")
        sys.exit(1)
        
    datalake_root = sys.argv[1] 
    extract_player_stats(datalake_root)