import requests
import json
import os
import sys
import time
import glob
import shutil
from datetime import datetime, timedelta

API_KEY = "9e001ae9e38e3f2945c55ad706e7d687"
BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": API_KEY}

LEAGUE_ID = 61
SEASON = 2024  # Ligue 1 2024/2025 => season=2024

FALLBACK_TEAM_IDS = [77,79,80,81,82,83,84,85,91,93,94,95,96,106,108,111,112,116,1063]


def call_api(endpoint: str, params: dict, tries: int = 3, sleep_s: int = 2):
    url = f"{BASE_URL}/{endpoint}"
    last_err = None
    for i in range(1, tries + 1):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            data = r.json()

            # retry sur erreurs serveurs / rate limit
            if r.status_code in (429, 500, 502, 503, 504):
                print(f"⚠️ {endpoint} status={r.status_code} try {i}/{tries} -> retry in {sleep_s*i}s")
                time.sleep(sleep_s * i)
                continue

            return data, r.status_code, None

        except Exception as e:
            last_err = e
            print(f"⚠️ {endpoint} exception try {i}/{tries}: {e} -> retry in {sleep_s*i}s")
            time.sleep(sleep_s * i)

    return None, None, last_err


def quota_reached(api_data: dict) -> bool:
    if not api_data:
        return False
    errs = api_data.get("errors") or {}
    msg = ""
    if isinstance(errs, dict):
        msg = str(errs.get("requests", "")).lower()
    return "request limit" in msg or "reached the request limit" in msg


def reuse_last_squads(datalake_path: str, date_str_raw: str) -> bool:
    """Copie le dernier squads_combined.json disponible (hors date courante) vers la date courante.
       Si le fichier du jour existe déjà, on ne copie pas et on retourne True.
    """
    base_dir = os.path.join(datalake_path, "raw/api_football/players_squads")
    out_dir = os.path.join(base_dir, date_str_raw)
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "squads_combined.json")

    # Si le fichier du jour existe déjà, on considère que c'est OK (rien à faire)
    if os.path.exists(out_file) and os.path.getsize(out_file) > 0:
        print(f" Quota atteint → squads du jour déjà présent : {out_file} (pas de copie)")
        return True

    # Cherche un squads_combined.json dans une autre date
    pattern = os.path.join(base_dir, "*", "squads_combined.json")
    files = sorted(glob.glob(pattern))

    # Exclure la date courante
    files = [f for f in files if f"/{date_str_raw}/" not in f.replace("\\", "/")]

    if not files:
        return False

    last_file = files[-1]

    # sécurité: éviter SameFileError
    if os.path.abspath(last_file) == os.path.abspath(out_file):
        print(f" Quota atteint → src == dst (rien à faire) : {out_file}")
        return True

    shutil.copy2(last_file, out_file)
    print(f" Quota atteint → réutilisation du dernier squads: {last_file} -> {out_file}")
    return True


def extract_squads(datalake_path: str, date_str_raw: str) -> None:
    raw_path = os.path.join(datalake_path, f"raw/api_football/players_squads/{date_str_raw}")
    os.makedirs(raw_path, exist_ok=True)

    print(f"--- Extraction squads API-Football | Ligue 1 season={SEASON} ---")

    # 1) Teams
    teams_data, status, err = call_api("teams", {"league": str(LEAGUE_ID), "season": str(SEASON)})

    if teams_data:
        print(f"INFO /teams status={status} results={teams_data.get('results')} errors={teams_data.get('errors')}")
        if quota_reached(teams_data):
            # fallback smart: réutiliser le dernier fichier existant
            if reuse_last_squads(datalake_path, date_str_raw):
                return
            raise RuntimeError("Quota API atteint et aucun squads précédent trouvé pour fallback.")
    else:
        print(f" /teams failed: {err}")

    teams = (teams_data.get("response") or []) if teams_data else []
    if not teams:
        print("⚠️ /teams a retourné 0 équipe. Fallback sur FALLBACK_TEAM_IDS.")
        team_list = [{"team": {"id": tid, "name": f"team_{tid}"}} for tid in FALLBACK_TEAM_IDS]
    else:
        team_list = teams

    # 2) Squads
    all_team_blocks = []
    ok = 0
    for idx, t in enumerate(team_list, 1):
        team = t.get("team") or {}
        team_id = team.get("id")
        team_name = team.get("name", f"team_{team_id}")
        if not team_id:
            continue

        print(f"[{idx}/{len(team_list)}] Squad: {team_name} (id={team_id})")

        squad_data, s_status, s_err = call_api("players/squads", {"team": str(team_id)})

        if squad_data and quota_reached(squad_data):
            if reuse_last_squads(datalake_path, date_str_raw):
                return
            raise RuntimeError("Quota API atteint pendant /players/squads et aucun squads précédent trouvé.")

        resp = (squad_data.get("response") or []) if squad_data else []
        if not resp:
            print(f"⚠️ squad vide pour team_id={team_id} status={s_status} err={s_err} errors={squad_data.get('errors') if squad_data else None}")
            continue

        all_team_blocks.extend(resp)
        ok += 1
        time.sleep(0.5)

    if ok == 0:
        # si vraiment rien récupéré => fallback fichier précédent si dispo
        if reuse_last_squads(datalake_path, date_str_raw):
            return
        raise RuntimeError("Aucune squad récupérée et aucun squads précédent trouvé.")

    out = {
        "get": "players/squads",
        "league": LEAGUE_ID,
        "season": SEASON,
        "date": date_str_raw,
        "results": len(all_team_blocks),
        "response": all_team_blocks,
    }

    filename = os.path.join(raw_path, "squads_combined.json")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f" Squads sauvegardés: {filename}")
    print(f" teams_ok = {ok} | blocks = {len(all_team_blocks)}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: extract_api_football_squads.py <datalake_path> [YYYYMMDD]")
        sys.exit(1)

    datalake_path = sys.argv[1]
    date_str_raw = sys.argv[2] if len(sys.argv) >= 3 else (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    extract_squads(datalake_path, date_str_raw)