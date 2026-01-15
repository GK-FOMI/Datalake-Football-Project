import requests
import json
import os
import sys
import time
from datetime import datetime, timedelta

def extract_player_stats(datalake_path):
    API_KEY = "9e001ae9e38e3f2945c55ad706e7d687" # Votre clé
    BASE_URL = "https://v3.football.api-sports.io/" 
    
    HEADERS = {
        "X-RapidAPI-Key": API_KEY,
        "X-RapidAPI-Host": "api-football-beta.p.rapidapi.com"
    }

    yesterday = datetime.now() - timedelta(days=1)
    date_str_raw = yesterday.strftime("%Y%m%d")
    
    # Configuration Ligue 1
    PARAMS = {
        "league": "61",  
        "season": "2024", 
        "page": 1 # On commence page 1
    }

    raw_path = os.path.join(datalake_path, f"raw/api_football/players_stats/{date_str_raw}")
    os.makedirs(raw_path, exist_ok=True)
    
    all_players = []
    
    try:
        print(f"--- Début extraction API (Ligue 1) ---")
        
        # On va chercher 5 pages (soit 100 joueurs) pour être sûr d'avoir les stars
        # Vous pouvez augmenter à 10 ou 20 si besoin, attention à la limite de 100 appels/jour
        for page in range(1, 6): 
            PARAMS["page"] = page
            print(f"Appel page {page}...")
            
            response = requests.get(f"{BASE_URL}/players", headers=HEADERS, params=PARAMS)
            response.raise_for_status()
            
            data = response.json()
            players_list = data.get('response', [])
            
            if not players_list:
                print("Plus de joueurs trouvés.")
                break
                
            all_players.extend(players_list)
            time.sleep(1) # Pause respectueuse pour l'API

        # Structure finale simulant la réponse API pour ne pas casser le script suivant
        final_data = {
            "get": "players",
            "parameters": PARAMS,
            "results": len(all_players),
            "paging": {"current": 1, "total": 1},
            "response": all_players
        }

        filename = os.path.join(raw_path, f"stats_combined.json")
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=4)
        
        print(f"Extraction terminée. {len(all_players)} joueurs sauvegardés dans {filename}")

    except Exception as e:
        print(f"Erreur extraction : {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)
    extract_player_stats(sys.argv[1])