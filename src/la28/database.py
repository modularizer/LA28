"""
Database connection management using SQLModel.
"""

from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from sqlmodel import SQLModel, Session as DBSession, create_engine, select

from sqlalchemy import text

from la28.models import (
    Day, Zone, Venue, Sport, EventType, Session, Event,
    SportVenueLink, get_default_event_types
)
from la28.schema import SCHEDULE_VIEW_SQL


class Database:
    """SQLModel database wrapper."""

    def __init__(self, db_path: str | Path = ":memory:", echo: bool = False):
        if db_path == ":memory:":
            self.db_url = "sqlite:///:memory:"
        else:
            self.db_url = f"sqlite:///{db_path}"
        self.engine = create_engine(self.db_url, echo=echo)

    def init(self) -> None:
        """Create all tables, views, and seed default data."""
        SQLModel.metadata.create_all(self.engine)
        self._seed_event_types()
        self._create_views()

    def reset(self) -> None:
        """Drop and recreate all tables and views."""
        with self.engine.connect() as conn:
            conn.execute(text("DROP VIEW IF EXISTS schedule_view"))
            conn.commit()
        SQLModel.metadata.drop_all(self.engine)
        SQLModel.metadata.create_all(self.engine)
        self._seed_event_types()
        self._create_views()

    def _seed_event_types(self) -> None:
        with self.session() as s:
            for et in get_default_event_types():
                existing = s.get(EventType, et.type)
                if not existing:
                    s.add(EventType(type=et.type, rank=et.rank))
            s.commit()

    def _create_views(self) -> None:
        """Create SQL views."""
        with self.engine.connect() as conn:
            conn.execute(text(SCHEDULE_VIEW_SQL))
            conn.commit()

    @contextmanager
    def session(self) -> Generator[DBSession, None, None]:
        with DBSession(self.engine) as session:
            yield session

    # =========================================================================
    # Zone
    # =========================================================================

    def get_zone(self, zone: str) -> Zone | None:
        with self.session() as s:
            return s.get(Zone, zone)

    def get_or_create_zone(self, zone: str) -> Zone:
        with self.session() as s:
            zone = s.get(Zone, zone)
            if zone:
                return zone
            zone = Zone(zone=zone)
            s.add(zone)
            s.commit()
            s.refresh(zone)
            return zone

    def all_zones(self) -> list[Zone]:
        with self.session() as s:
            return list(s.exec(select(Zone).order_by(Zone.zone)).all())

    # =========================================================================
    # Venue
    # =========================================================================

    def get_venue(self, venue: str) -> Venue | None:
        with self.session() as s:
            return s.get(Venue, venue)

    def get_or_create_venue(self, venue: str, zone: str, in_okc: bool = False) -> Venue:
        with self.session() as s:
            venue = s.get(Venue, venue)
            if venue:
                return venue
            venue = Venue(venue=venue, zone=zone, in_okc=in_okc)
            s.add(venue)
            s.commit()
            s.refresh(venue)
            return venue

    def all_venues(self) -> list[Venue]:
        with self.session() as s:
            return list(s.exec(select(Venue).order_by(Venue.venue)).all())

    # =========================================================================
    # Sport
    # =========================================================================

    def get_sport(self, sport: str) -> Sport | None:
        with self.session() as s:
            return s.get(Sport, sport)

    def get_or_create_sport(self, sport: str) -> Sport:
        with self.session() as s:
            sport = s.get(Sport, sport)
            if sport:
                return sport
            sport = Sport(sport=sport)
            s.add(sport)
            s.commit()
            s.refresh(sport)
            return sport

    def all_sports(self) -> list[Sport]:
        with self.session() as s:
            return list(s.exec(select(Sport).order_by(Sport.sport)).all())

    # =========================================================================
    # EventType
    # =========================================================================

    def get_event_type(self, _type: str) -> EventType | None:
        with self.session() as s:
            return s.get(EventType, _type)

    def all_event_types(self) -> list[EventType]:
        with self.session() as s:
            return list(s.exec(select(EventType).order_by(EventType.rank)).all())

    # =========================================================================
    # SportVenue link
    # =========================================================================

    def link_sport_venue(self, sport: str, venue: str, is_primary: bool = False) -> None:
        with self.session() as s:
            existing = s.exec(
                select(SportVenueLink)
                .where(SportVenueLink.sport == sport)
                .where(SportVenueLink.venue == venue)
            ).first()
            if not existing:
                s.add(SportVenueLink(sport=sport, venue=venue, is_primary=is_primary))
                s.commit()

    # =========================================================================
    # Session
    # =========================================================================

    def add_session(self, sess: Session) -> Session:
        with self.session() as s:
            s.add(sess)
            s.commit()
            s.refresh(sess)
            return sess

    def get_session(self, code: str) -> Session | None:
        with self.session() as s:
            return s.get(Session, code)


    def all_sessions(self) -> list[Session]:
        with self.session() as s:
            return list(s.exec(select(Session).order_by(Session.starts_at)).all())

    # =========================================================================
    # Event
    # =========================================================================

    def add_event(self, event: Event) -> Event:
        with self.session() as s:
            s.add(event)
            s.commit()
            s.refresh(event)
            return event

    def all_events(self) -> list[Event]:
        with self.session() as s:
            return list(s.exec(select(Event).order_by(Event.event_number)).all())

    # =========================================================================
    # Stats
    # =========================================================================

    def stats(self) -> dict:
        with self.session() as s:
            return {
                "days": len(s.exec(select(Day)).all()),
                "zones": len(s.exec(select(Zone)).all()),
                "venues": len(s.exec(select(Venue)).all()),
                "sports": len(s.exec(select(Sport)).all()),
                "sessions": len(s.exec(select(Session)).all()),
                "events": len(s.exec(select(Event)).all()),
            }

    def create_views(self) -> None:
        """Create SQL views (can be called on existing database)."""
        self._create_views()

    def load_osm_venues(self, json_path: str) -> dict:
        """
        Load venue geocoding data from OSM JSON file.

        Updates venue records with address, latitude, and longitude.

        Args:
            json_path: Path to venues_osm.json file

        Returns:
            Dict with counts of updated, skipped, and not_found venues
        """
        import json

        with open(json_path, "r", encoding="utf-8") as f:
            venues_data = json.load(f)

        updated = 0
        skipped = 0
        not_found = 0

        with self.session() as s:
            for item in venues_data:
                name = item.get("name")
                status = item.get("status")
                address = item.get("address")
                lat_lng = item.get("lat_lng", {})
                lat = lat_lng.get("lat")
                lng = lat_lng.get("lng")

                if status in ("unlocatable", "not_found") or not name:
                    skipped += 1
                    continue

                venue = s.get(Venue, name)
                if not venue:
                    not_found += 1
                    continue

                venue.address = address
                venue.latitude = lat
                venue.longitude = lng
                s.add(venue)
                updated += 1

            s.commit()

        return {"updated": updated, "skipped": skipped, "not_found": not_found}
