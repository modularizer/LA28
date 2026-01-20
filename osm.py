"""
Foolproof-ish Nominatim geocoder for LA28 venues.

Upgrade: adds an in-memory cache so if we already searched the same query,
we reuse the results (no extra Nominatim calls, no extra sleep).
"""

from __future__ import annotations

import json
import random
import time
from typing import Any, Optional

import requests

# ----------------------------
# Config
# ----------------------------
EMAIL = "modularizer@gmail.com"
USER_AGENT = f"LA28VenueGeocoder/1.2 ({EMAIL})"

NOMINATIM_SEARCH = "https://nominatim.openstreetmap.org/search"
NOMINATIM_REVERSE = "https://nominatim.openstreetmap.org/reverse"

BASE_DELAY_S = 1.2
MAX_RETRIES = 6
TIMEOUT_S = 30

DEFAULT_CONTEXT = "Los Angeles, California, USA"

# ----------------------------
# Disambiguation hints
# ----------------------------
VENUES: dict[str, Optional[str]] = {
    "Honda Center": "2695 E Katella Ave, Anaheim, CA 92806",
    "Carson Velodrome": "18400 Avalon Blvd, Carson, CA 90746",
    "Valley Complex 1": "Sepulveda Basin Sports Complex",
    "Valley Complex 2": "Sepulveda Basin Sports Complex",
    "Valley Complex 3": "Sepulveda Basin Sports Complex",
    "Valley Complex 4": "Sepulveda Basin Sports Complex",
    "Long Beach Arena": "Long Beach Arena",
    "Long Beach Climbing Theater": "Long Beach Convention Center",
    "Long Beach Target Shooting Hall": "Long Beach Convention Center",
    "Long Beach Aquatics Center": "Long Beach Convention Center",
    "Alamitos Beach Stadium": "Alamitos Beach",
    "Belmont Shore": "Belmont Shore Beach",
    "Marine Stadium": "Long Beach Marine Stadium",
    "Exposition Park Stadium": "BMO Stadium",
    "LA Memorial Coliseum": "Los Angeles Memorial Coliseum",
    "Rose Bowl Stadium": "Rose Bowl Stadium",
    "Rose Bowl Aquatics Center": "Rose Bowl Aquatics Center, Pasadena, CA",
    "Dodger Stadium": "Dodger Stadium",
    "Galen Center": "Galen Center",
    "Peacock Theater": "Peacock Theater Los Angeles",
    "DTLA Arena": "Crypto.com Arena",
    "LA Convention Center Hall 1": "Los Angeles Convention Center",
    "LA Convention Center Hall 2": "Los Angeles Convention Center",
    "LA Convention Center Hall 3": "Los Angeles Convention Center",
    "Carson Stadium": "Dignity Health Sports Park, Carson, CA",
    "Carson Field": "Dignity Health Sports Park, Carson, CA",
    "Carson Center Court": "Dignity Health Sports Park, Carson, CA",
    "Carson Court 1": "Dignity Health Sports Park, Carson, CA",
    "Carson Court 2": "Dignity Health Sports Park, Carson, CA",
    "Carson Courts 3-11": "Dignity Health Sports Park, Carson, CA",
    "Venice Beach": "Venice Beach",
    "Venice Beach Boardwalk - Start": "Venice Beach Boardwalk",
    "Riviera Country Club": "Riviera Country Club",
    "Santa Anita Park": "Santa Anita Park",
    "Inglewood Dome": "SoFi Stadium",
    "2028 Stadium": "SoFi Stadium",
    "Port of Los Angeles": "Port of Los Angeles",
    "Industry Hills MTB Course": "1 Industry Hills Pkwy, City of Industry, CA 91744",
    "Whittier Narrows Clay Shooting Center": "Whittier Narrows Recreation Area",
    "Trestles State Beach": "San Onofre State Beach, San Clemente, CA",
    "Comcast Squash Center at Universal Studios": "Universal Studios Hollywood",
    "Fairgrounds Cricket Stadium": "Pomona Fairplex",
    "OKC Softball Park": "Devon Park, Oklahoma City, OK",
    "OKC Whitewater Center": "Riversport Rapids",

    "N/A": None,
    "TBD": None,
}

# ----------------------------
# HTTP client + helpers
# ----------------------------
session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "en"})


def sleep_polite(mult: float = 1.0) -> None:
    time.sleep(BASE_DELAY_S * mult + random.random() * 0.25)


def request_with_backoff(url: str, params: dict[str, Any]) -> Any:
    last_status: Optional[int] = None
    last_text: Optional[str] = None

    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, params=params, timeout=TIMEOUT_S)
            last_status = r.status_code
            last_text = r.text[:500] if r.text else None

            if r.status_code in (403, 429, 503):
                retry_after = r.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait_s = float(retry_after)
                    except ValueError:
                        wait_s = (2**attempt) + random.random()
                else:
                    wait_s = (2**attempt) + random.random()

                print(f"Blocked/rate-limited ({r.status_code}). Backing off {wait_s:.1f}s…")
                time.sleep(wait_s)
                continue

            r.raise_for_status()
            return r.json()

        except requests.Timeout:
            wait_s = (2**attempt) + random.random()
            print(f"Timeout. Backing off {wait_s:.1f}s…")
            time.sleep(wait_s)

        except requests.RequestException as e:
            wait_s = (2**attempt) + random.random()
            print(f"Request error ({type(e).__name__}). Backing off {wait_s:.1f}s…")
            time.sleep(wait_s)

        except ValueError:
            wait_s = (2**attempt) + random.random()
            print(f"Bad JSON response. Backing off {wait_s:.1f}s…")
            time.sleep(wait_s)

    raise RuntimeError(
        f"Request failed after retries (status={last_status}) url={url} params={params} "
        f"response_snippet={last_text!r}"
    )


# ----------------------------
# Cached Nominatim queries
# ----------------------------
_SEARCH_CACHE: dict[tuple[str, int], list[dict[str, Any]]] = {}
_REVERSE_CACHE: dict[tuple[float, float, int], dict[str, Any]] = {}


def nominatim_search(query: str, limit: int = 5) -> list[dict[str, Any]]:
    """
    Cached by (query, limit). If cached, no HTTP call and no sleep.
    """
    key = (query, limit)
    if key in _SEARCH_CACHE:
        return _SEARCH_CACHE[key]

    params = {
        "q": query,
        "format": "jsonv2",
        "addressdetails": 1,
        "limit": limit,
        "email": EMAIL,
    }
    data = request_with_backoff(NOMINATIM_SEARCH, params=params)
    sleep_polite(1.0)

    results = data if isinstance(data, list) else []
    _SEARCH_CACHE[key] = results
    return results


def nominatim_reverse(lat: float, lon: float, zoom: int = 18) -> dict[str, Any]:
    """
    Cached by (lat, lon, zoom). If cached, no HTTP call and no sleep.
    """
    key = (round(lat, 7), round(lon, 7), zoom)
    if key in _REVERSE_CACHE:
        return _REVERSE_CACHE[key]

    params = {
        "lat": lat,
        "lon": lon,
        "format": "jsonv2",
        "addressdetails": 1,
        "zoom": zoom,
        "email": EMAIL,
    }
    data = request_with_backoff(NOMINATIM_REVERSE, params=params)
    sleep_polite(1.0)

    result = data if isinstance(data, dict) else {}
    _REVERSE_CACHE[key] = result
    return result


# ----------------------------
# Selection + formatting
# ----------------------------
def _safe_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def choose_best(results: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not results:
        return None

    def score(x: dict[str, Any]) -> float:
        imp = _safe_float(x.get("importance"))
        cls = (x.get("class") or "").lower()
        typ = (x.get("type") or "").lower()
        addr = x.get("address") or {}

        poi_bonus = 0.15 if cls in {"amenity", "tourism", "leisure", "building", "sport"} else 0.0
        generic_penalty = -0.10 if cls in {"boundary", "place"} else 0.0
        has_road = 0.08 if (addr.get("road") or addr.get("pedestrian") or addr.get("footway")) else 0.0
        has_number = 0.06 if addr.get("house_number") else 0.0
        venue_word_bonus = 0.05 if any(k in typ for k in ("stadium", "arena", "sports_centre", "pitch")) else 0.0

        return imp + poi_bonus + venue_word_bonus + has_road + has_number + generic_penalty

    return sorted(results, key=score, reverse=True)[0]


def ambiguity_flag(results: list[dict[str, Any]]) -> bool:
    if len(results) < 2:
        return False
    i0 = _safe_float(results[0].get("importance"))
    i1 = _safe_float(results[1].get("importance"))
    return abs(i0 - i1) < 0.02


def looks_like_wrong_country(best: dict[str, Any]) -> bool:
    addr = best.get("address") or {}
    cc = (addr.get("country_code") or "").lower()
    return bool(cc) and cc != "us"


# ----------------------------
# Core geocode
# ----------------------------
def build_query(lookup_name: str) -> str:
    lower = lookup_name.lower()

    # If caller already included a city/state, don't add DEFAULT_CONTEXT
    if any(x in lower for x in (", ca", "california", "pasadena", "carson", "inglewood", "long beach", "san clemente")):
        return lookup_name

    if any(k in lower for k in ("okc", "oklahoma", "riversport", "softball hall of fame")):
        return lookup_name

    return f"{lookup_name}, {DEFAULT_CONTEXT}"


def geocode_venue(schedule_name: str, lookup_name: Optional[str]) -> dict[str, Any]:
    if not lookup_name:
        return {
            "name": schedule_name,
            "status": "unlocatable",
            "address": None,
            "lat_lng": {"lat": None, "lng": None},
            "debug": {"query": None, "candidates": []},
        }

    query = build_query(lookup_name)
    results = nominatim_search(query, limit=5)
    best = choose_best(results)

    if not best:
        return {
            "name": schedule_name,
            "status": "not_found",
            "address": None,
            "lat_lng": {"lat": None, "lng": None},
            "debug": {"query": query, "candidates": results},
        }

    lat = _safe_float(best.get("lat"))
    lon = _safe_float(best.get("lon"))
    address = best.get("display_name")

    status = "ok"
    if ambiguity_flag(results) or looks_like_wrong_country(best) or (lat == 0.0 and lon == 0.0):
        status = "needs_review"

    return {
        "name": schedule_name,
        "status": status,
        "address": address,
        "lat_lng": {"lat": lat, "lng": lon},
        "debug": {
            "query": query,
            "picked": {
                "display_name": best.get("display_name"),
                "lat": best.get("lat"),
                "lon": best.get("lon"),
                "class": best.get("class"),
                "type": best.get("type"),
                "importance": best.get("importance"),
                "osm_type": best.get("osm_type"),
                "osm_id": best.get("osm_id"),
                "address": best.get("address"),
            },
            "candidates": results,
            "cache_stats": {
                "search_cache_size": len(_SEARCH_CACHE),
                "reverse_cache_size": len(_REVERSE_CACHE),
            },
        },
    }


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    clean: list[dict[str, Any]] = []
    review: list[dict[str, Any]] = []

    items = list(VENUES.items())
    total = len(items)

    for i, (schedule_name, lookup_name) in enumerate(items, 1):
        item = geocode_venue(schedule_name, lookup_name)
        review.append(item)
        clean.append(
            {
                "name": item["name"],
                "address": item["address"],
                "lat_lng": item["lat_lng"],
            }
        )

        print(f"[{i}/{total}] {schedule_name} -> {item['status']}")

        # Only sleep between venues if we might have made network calls.
        # (Even if cached, a small pause is fine, but we keep it minimal.)
        sleep_polite(0.4)

    with open("resources/venues_osm.json", "w", encoding="utf-8") as f:
        json.dump(clean, f, indent=2, ensure_ascii=False)


    print("Wrote venues_osm.json")


if __name__ == "__main__":
    main()
