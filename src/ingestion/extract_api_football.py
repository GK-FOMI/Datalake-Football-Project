import requests
import json
import os
import sys
import time
from datetime import datetime, timedelta


def api_get(base_url: str, headers: dict, endpoint: str, params: dict, timeout: int = 30) -> dict:
    r = requests.get(f"{base_url}{endpoint}", headers=headers, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def extract_players_by_teams(datalake_path: str, date_str_raw: str) -> None:
    API_KEY = "9e001ae9e38e3f2945c55ad706e7d687"
    BASE_URL = "https://v3.football.api-sports.io"
    HEADERS = {"x-apisports-key": API_KEY}

    LEAGUE_ID = 61       # Ligue 1
    SEASON = 2024        # 2024/2025 côté API-Football

    raw_path = os.path.join(datalake_path, f"raw/api_football/players_stats/{date_str_raw}")
    os.makedirs(raw_path, exist_ok=True)

    print(f"--- Début extraction API par équipes (league={LEAGUE_ID}, season={SEASON}) pour {date_str_raw} ---")

    # 1) Récupérer les équipes
    teams_payload = api_get(
        BASE_URL, HEADERS, "/teams",
        params={"league": str(LEAGUE_ID), "season": str(SEASON)}
    )
    teams = teams_payload.get("response", [])
    if not teams:
        raise RuntimeError("Aucune équipe retournée par /teams (vérifie league/season).")

    team_ids = []
    for t in teams:
        tid = (t.get("team") or {}).get("id")
        tname = (t.get("team") or {}).get("name")
        if tid is not None:
            team_ids.append((tid, tname))

    print(f" {len(team_ids)} équipes trouvées.")

    # 2) Pour chaque équipe, récupérer tous les joueurs (pagination)
    all_players = []
    errors = []

    for idx, (team_id, team_name) in enumerate(team_ids, start=1):
        print(f"[{idx}/{len(team_ids)}] Équipe {team_name} (id={team_id})")

        # page 1 pour connaître paging.total
        base_params = {"team": str(team_id), "season": str(SEASON), "page": 1}
        first = api_get(BASE_URL, HEADERS, "/players", params=base_params)
        resp1 = first.get("response", [])
        paging = first.get("paging", {}) or {}
        total_pages = int(paging.get("total", 1) or 1)

        all_players.extend(resp1)

        # pages 2..N
        for page in range(2, total_pages + 1):
            try:
                params = {"team": str(team_id), "season": str(SEASON), "page": page}
                data = api_get(BASE_URL, HEADERS, "/players", params=params)
                all_players.extend(data.get("response", []))
                time.sleep(0.6)  # petit throttle
            except Exception as e:
                errors.append({"team_id": team_id, "team_name": team_name, "page": page, "error": str(e)})

        time.sleep(0.8)  # throttle entre équipes

    final_data = {
        "get": "players_by_teams",
        "parameters": {"league": LEAGUE_ID, "season": SEASON},
        "results": len(all_players),
        "errors": errors,
        "response": all_players,
    }

    filename = os.path.join(raw_path, "stats_combined.json")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(final_data, f, ensure_ascii=False, indent=2)

    print(f" Extraction terminée. {len(all_players)} entrées joueurs/statistiques -> {filename}")
    if errors:
        print(f"⚠️ {len(errors)} erreurs pendant la pagination (voir champ 'errors' dans le JSON).")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: extract_api_football_by_teams.py <datalake_path> [YYYYMMDD]")
        sys.exit(1)

    datalake_path = sys.argv[1]
    date_str_raw = sys.argv[2] if len(sys.argv) >= 3 else (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    try:
        extract_players_by_teams(datalake_path, date_str_raw)
    except Exception as e:
        print(f" Erreur extraction : {e}")
        sys.exit(1)