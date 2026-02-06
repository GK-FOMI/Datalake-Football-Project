import requests
from bs4 import BeautifulSoup
import json
import os
import sys
from datetime import datetime, timedelta

def extract_market_values(datalake_path, date_str_raw):
    URL = "https://www.transfermarkt.fr/ligue-1/marktwerte/wettbewerb/FR1?saison_id=2024"
    HEADERS = {
        "User-Agent": "Mozilla/5.0"
    }

    raw_path = os.path.join(datalake_path, f"raw/transfermarkt/market_values/{date_str_raw}")
    os.makedirs(raw_path, exist_ok=True)
    filename = os.path.join(raw_path, "values_scraped.json")

    print(f"--- Début scraping Transfermarkt pour {date_str_raw} ---")

    r = requests.get(URL, headers=HEADERS)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.select_one("table.items")
    rows = table.select("tbody tr")

    data = []

    for tr in rows:
        name_td = tr.select_one("td.hauptlink a")
        value_td = tr.select("td.rechts")

        if not name_td or not value_td:
            continue

        player_name = name_td.text.strip()
        market_value_raw = value_td[-1].text.strip()

        data.append({
            "player_name": player_name,
            "market_value_raw": market_value_raw,
            "scraped_url": URL
        })

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    print(f"Scraping terminé : {len(data)} joueurs sauvegardés")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: extract_transfermarkt.py <datalake_path> [YYYYMMDD]")
        sys.exit(1)

    datalake_path = sys.argv[1]
    date_str_raw = sys.argv[2] if len(sys.argv) >= 3 else (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    extract_market_values(datalake_path, date_str_raw)