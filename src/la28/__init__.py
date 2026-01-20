"""
LA28 Olympics Schedule Database

SQLModel-based database for managing LA28 Olympic Games schedule data.

Usage:
    from la28_db import Database, load_from_pdf, export_all

    # Create and initialize database
    db = Database("la28.db")
    db.init()

    # Load from PDF (primary method)
    stats = load_from_pdf(db)

    # Or load from existing JSON
    stats = load_from_json(db, "sessions.json")

    # Query data
    with db.session() as s:
        from la28_db import SessionQuery
        sessions = SessionQuery(s).by_sport("Swimming").by_day(1).fetch()

    # Export data
    export_all(db, "./output")
"""

from .models import (
    Zone,
    Venue,
    Sport,
    EventType,
    Session,
    Event,
    SportVenueLink,
    ScheduleView,
    get_default_event_types,
)

from .database import Database

from .parsing import (
    load_from_json,
)

from .queries import SessionQuery, EventQuery, ScheduleQuery

from .export import (
    export_sessions_json,
    export_sessions_csv,
    export_events_json,
    export_events_csv,
    export_sports_json,
    export_sports_csv,
    export_venues_json,
    export_venues_csv,
    export_zones_json,
    export_zones_csv,
    export_xlsx,
    export_all,
)

from .schema import (
    generate_schema,
    generate_sqlite_schema,
    generate_postgres_schema,
    write_schema,
)

__all__ = [
    # Models
    "Zone",
    "Venue",
    "Sport",
    "EventType",
    "Session",
    "Event",
    "SportVenueLink",
    "ScheduleView",
    "get_default_event_types",
    # Database
    "Database",
    # Parsing
    "load_from_json",
    # Queries
    "SessionQuery",
    "EventQuery",
    "ScheduleQuery",
    # Export
    "export_sessions_json",
    "export_sessions_csv",
    "export_events_json",
    "export_events_csv",
    "export_sports_json",
    "export_sports_csv",
    "export_venues_json",
    "export_venues_csv",
    "export_zones_json",
    "export_zones_csv",
    "export_xlsx",
    "export_all",
    # Schema
    "generate_schema",
    "generate_sqlite_schema",
    "generate_postgres_schema",
    "write_schema",
]
