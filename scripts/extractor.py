# scripts/extractor.py
import requests
import json
import os
from pathlib import Path
from datetime import date, datetime


API_HOST = "api-football-v1.p.rapidapi.com"
BASE_URL = f"https://{API_HOST}/v3"
WC_LEAGUE_ID = 1        # FIFA World Cup en API-Football
WC_SEASON    = 2026


def _get_headers(api_key: str) -> dict:
    """Construye los headers necesarios para cada request."""
    return {
        "X-RapidAPI-Key":  api_key,
        "X-RapidAPI-Host": API_HOST
    }


def _save_bronze(endpoint: str, data: dict, suffix: str = "") -> Path:
    """
    Guarda el JSON raw en la capa Bronze.
    Ruta: data/bronze/{endpoint}/{YYYY-MM-DD}_{suffix}.json
    """
    today = date.today().isoformat()
    folder = Path(f"data/bronze/{endpoint}")
    folder.mkdir(parents=True, exist_ok=True)

    filename = f"{today}_{suffix}.json" if suffix else f"{today}.json"
    out_path = folder / filename

    payload = {
        "extracted_at": datetime.utcnow().isoformat(),
        "endpoint": endpoint,
        "suffix": suffix,
        "data": data
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"[Bronze] Guardado: {out_path}")
    return out_path


def extract_fixtures(api_key: str) -> list:
    """
    Extrae todos los fixtures del Mundial para la fecha de hoy.
    Retorna lista de fixture_ids que tienen status FT (Final Time).
    """
    today = date.today().isoformat()
    resp = requests.get(
        f"{BASE_URL}/fixtures",
        headers=_get_headers(api_key),
        params={
            "league":  WC_LEAGUE_ID,
            "season":  WC_SEASON,
            "date":    today
        },
        timeout=30
    )
    resp.raise_for_status()
    data = resp.json()
    _save_bronze("fixtures", data)

    # Retornar solo IDs de partidos terminados
    finished = [
        f["fixture"]["id"]
        for f in data.get("response", [])
        if f["fixture"]["status"]["short"] == "FT"
    ]
    print(f"[Bronze] Partidos terminados hoy: {len(finished)} -> {finished}")
    return finished


def extract_match_statistics(api_key: str, fixture_id: int) -> None:
    """Extrae estadisticas del partido (posesion, tiros, corners, etc.)"""
    resp = requests.get(
        f"{BASE_URL}/fixtures/statistics",
        headers=_get_headers(api_key),
        params={"fixture": fixture_id},
        timeout=30
    )
    resp.raise_for_status()
    _save_bronze("statistics", resp.json(), suffix=str(fixture_id))


def extract_lineups(api_key: str, fixture_id: int) -> None:
    """Extrae alineaciones y formaciones de ambos equipos."""
    resp = requests.get(
        f"{BASE_URL}/fixtures/lineups",
        headers=_get_headers(api_key),
        params={"fixture": fixture_id},
        timeout=30
    )
    resp.raise_for_status()
    _save_bronze("lineups", resp.json(), suffix=str(fixture_id))


def extract_player_stats(api_key: str, fixture_id: int) -> None:
    """Extrae estadisticas individuales de cada jugador en el partido."""
    resp = requests.get(
        f"{BASE_URL}/fixtures/players",
        headers=_get_headers(api_key),
        params={"fixture": fixture_id},
        timeout=30
    )
    resp.raise_for_status()
    _save_bronze("players", resp.json(), suffix=str(fixture_id))


def extract_standings(api_key: str) -> None:
    """Extrae la tabla de posiciones actual de los grupos."""
    resp = requests.get(
        f"{BASE_URL}/standings",
        headers=_get_headers(api_key),
        params={"league": WC_LEAGUE_ID, "season": WC_SEASON},
        timeout=30
    )
    resp.raise_for_status()
    _save_bronze("standings", resp.json())
