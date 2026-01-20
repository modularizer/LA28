#!/usr/bin/env python3
"""Load LA28 data from PDF, export to parsed/, create SQLite database."""
from pathlib import Path

from la28 import Database, export_all, load_from_json

# Create database
parsed = Path("parsed")
parsed.mkdir(exist_ok=True)

db_path = parsed / "la28.db"
fresh=not db_path.exists()
# fresh=False
db = Database(db_path)
db.init()



if fresh:
    # Load schedule data (creates venues, sessions, events)
    print("Loading schedule data...")
    stats = load_from_json(db, "resources/la28-schedule.json")
    print(f"Loaded: {stats}")

    # Load OSM venue geocoding data (updates venues with lat/lng)
    print("Loading OSM venue data...")
    osm_stats = db.load_osm_venues("resources/venues_osm.json")
    print(f"OSM venues: {osm_stats}")


    # Export all CSVs and JSONs
    print("\nExporting to parsed/...")
    export_stats = export_all(db, parsed)
    for name, count in export_stats.items():
        print(f"  {name}: {count} records")

print(f"\nDatabase saved to parsed/la28.db")
print(f"Stats: {db.stats()}")
