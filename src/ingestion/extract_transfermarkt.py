import requests
from bs4 import BeautifulSoup
import json
import os
import sys
from datetime import datetime, timedelta

def extract_market_values(datalake_path):
    """
    Extrait la valeur marchande des joueurs par Scraping Web (Transfermarkt)
    et stocke les données brutes dans la couche RAW du Datalake.
    """
    # --- Configuration ---
    # URL de la page des valeurs marchandes (à ajuster si nécessaire)
    URL = "https://www.transfermarkt.fr/ligue-1/startseite/wettbewerb/FR1/saison_id/2024" 
    
    # Headers simulant un navigateur pour éviter le blocage par le site
    # Remplacez l'ancienne variable HEADERS par ceci :
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.google.com/'
    }

    yesterday = datetime.now() - timedelta(days=1)
    date_str_raw = yesterday.strftime("%Y%m%d")
    raw_path = os.path.join(datalake_path, f"raw/transfermarkt/market_values/{date_str_raw}")
    # --- Fin de Configuration ---

    try:
        os.makedirs(raw_path, exist_ok=True)
        filename = os.path.join(raw_path, f"values_scraped.json")

        print(f"Début du scraping de {URL}")

        # 1. Appel HTTP 
        response = requests.get(URL, headers=HEADERS)
        response.raise_for_status() 
        
        # 2. Parsing du HTML 
        soup = BeautifulSoup(response.content, 'html.parser')
        scraped_data = []

        # 3. Extraction : Trouver les lignes de joueurs (sélection à vérifier)
        # Ceci est très sensible aux changements de site.
        player_rows = soup.find_all('tr', class_=['odd', 'even']) 
        
        for row in player_rows:
            # Sélecteurs basiques pour l'exemple
            player_name_tag = row.find('td', class_='hauptlink').a if row.find('td', class_='hauptlink') else None
            value_tag = row.find('td', class_='rechts') 
            
            if player_name_tag and value_tag:
                scraped_data.append({
                    "player_name": player_name_tag.text.strip(),
                    "market_value_raw": value_tag.text.strip(),
                    "scraped_url": URL
                })

        # 4. Sauvegarde du JSON brut
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(scraped_data, f, ensure_ascii=False, indent=4)
        
        print(f"Scraping réussi. {len(scraped_data)} joueurs extraits.")

    except requests.exceptions.RequestException as e:
        print(f"Erreur HTTP/Connexion ou échec de l'appel : {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Erreur de parsing (HTML/Sélecteur incorrect) : {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)
    datalake_root = sys.argv[1] 
    extract_market_values(datalake_root)