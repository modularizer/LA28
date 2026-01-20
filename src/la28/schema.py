"""
Schema generation utilities.

Generate DDL for SQLite or PostgreSQL.
"""
from typing import Literal

from sqlalchemy import create_engine
from sqlalchemy.schema import CreateTable, CreateIndex
from sqlmodel import SQLModel

from la28.models import (
    Day, Zone, Venue, Sport, EventType, Session, Event, SportVenueLink
)

# Ensure all models are registered
_MODELS = [Day, Zone, Venue, Sport, EventType, Session, Event, SportVenueLink]

# SQL View joining events, sessions, venues, zones, sports
SCHEDULE_VIEW_SQL = """
CREATE VIEW IF NOT EXISTS schedule_view AS
SELECT
    -- Event fields
    event.id AS event_id,
    event.sex AS event_sex,
    event.description AS event_description,
    event.type AS event_type,
    event.order_in_session,
    event.total_in_session,
    event.event_number,
    event.total_events,

    -- Session fields
    session.code AS session_code,
    session.day,
    session.starts_at,
    session.ends_at,
    session.timezone,
    session.ticketed,
    session.type AS session_type,
    session.session_number,
    session.total_sessions,

    -- Sport fields
    sport.sport,

    -- Venue fields
    venue.venue,
    venue.address AS venue_address,
    venue.capacity AS venue_capacity,
    venue.latitude AS venue_latitude,
    venue.longitude AS venue_longitude,
    venue.in_okc,

    -- Zone fields
    zone.zone,
    zone.description AS zone_description,

    -- EventType rank
    event_type.rank AS event_type_rank,

    -- Map links (computed from lat/lng)
    CASE
        WHEN venue.latitude IS NOT NULL AND venue.longitude IS NOT NULL
        THEN 'https://www.google.com/maps/search/?api=1&query=' || venue.latitude || ',' || venue.longitude
        ELSE NULL
    END AS google_maps_url,

    CASE
        WHEN venue.latitude IS NOT NULL AND venue.longitude IS NOT NULL
        THEN 'https://waze.com/ul?ll=' || venue.latitude || ',' || venue.longitude || '&navigate=yes'
        ELSE NULL
    END AS waze_url,

    CASE
        WHEN venue.latitude IS NOT NULL AND venue.longitude IS NOT NULL
        THEN 'https://maps.apple.com/?ll=' || venue.latitude || ',' || venue.longitude || '&q=' || REPLACE(venue.venue, ' ', '+')
        ELSE NULL
    END AS apple_maps_url,

    CASE
        WHEN venue.latitude IS NOT NULL AND venue.longitude IS NOT NULL
        THEN 'https://www.openstreetmap.org/?mlat=' || venue.latitude || '&mlon=' || venue.longitude || '&zoom=17'
        ELSE NULL
    END AS osm_url

FROM event
JOIN session ON session.code = event.code
JOIN venue ON venue.venue = session.venue
JOIN zone ON zone.zone = venue.zone
JOIN sport ON sport.sport = session.sport
LEFT JOIN event_type ON event_type.type = event.type
ORDER BY session.starts_at, event.order_in_session;
"""

Dialect = Literal["sqlite", "postgresql"]
class Dialects:
    SQLITE: Dialect = "sqlite"
    POSTGRES: Dialect = "postgres"


def generate_schema(dialect: Dialect = Dialects.SQLITE) -> str:
    """
    Generate DDL schema for the specified database dialect.

    Args:
        dialect: "sqlite" or "postgresql"

    Returns:
        SQL DDL string
    """
    if dialect == "sqlite":
        engine = create_engine("sqlite:///:memory:")
    elif dialect in ("postgresql", "postgres"):
        engine = create_engine("postgresql://", strategy="mock", executor=lambda *a, **kw: None)
    else:
        raise ValueError(f"Unsupported dialect: {dialect}")

    lines = []

    # Header
    lines.append(f"-- LA28 Olympics Schedule Database Schema")
    lines.append(f"-- Generated for: {dialect}")
    lines.append("")

    # Create tables
    for table in SQLModel.metadata.sorted_tables:
        ddl = str(CreateTable(table).compile(engine))
        # Clean up the output
        ddl = ddl.strip()
        if not ddl.endswith(";"):
            ddl += ";"
        lines.append(ddl)
        lines.append("")

    # Create indexes (not part of CreateTable)
    for table in SQLModel.metadata.sorted_tables:
        for index in table.indexes:
            ddl = str(CreateIndex(index).compile(engine))
            ddl = ddl.strip()
            if not ddl.endswith(";"):
                ddl += ";"
            lines.append(ddl)

    lines.append("")

    # Seed data for event types
    lines.append("-- Seed event types")
    event_types = [
        ("Final", 1),
        ("Bronze", 2),
        ("Semifinal", 3),
        ("Quarterfinal", 4),
        ("Repechage", 5),
        ("Preliminary", 6),
        ("N/A", 99),
    ]
    for name, rank in event_types:
        lines.append(f"INSERT INTO event_type (name, rank) VALUES ('{name}', {rank});")

    lines.append("")
    lines.append("-- Schedule view (joins events, sessions, venues, zones, sports)")
    lines.append(SCHEDULE_VIEW_SQL)

    return "\n".join(lines)


def write_schema(path: str, dialect: Dialect = Dialects.SQLITE) -> None:
    """Write schema DDL to a file."""
    sql = generate_schema(dialect)
    with open(path, "w", encoding="utf-8") as f:
        f.write(sql)


def generate_sqlite_schema() -> str:
    """Generate SQLite-compatible schema."""
    return generate_schema(Dialects.SQLITE)


def generate_postgres_schema() -> str:
    """Generate PostgreSQL-compatible schema."""
    return generate_schema(Dialects.POSTGRES)
