"""
Query utilities for the LA28 database.
"""

from datetime import datetime, date

from sqlmodel import Session as DBSession, select, col, or_
from sqlalchemy.orm import selectinload

from la28.models import Zone, Venue, Sport, EventType, Session, Event


class SessionQuery:
    """Fluent query builder for sessions."""

    def __init__(self, db_session: DBSession):
        self._session = db_session
        self._query = select(Session)

    def with_relations(self) -> "SessionQuery":
        """Eager load related entities."""
        self._query = self._query.options(
            selectinload(Session.sport_rel),
            selectinload(Session.venue_rel).selectinload(Venue.zone_rel),
            selectinload(Session.type_rel),
            selectinload(Session.events).selectinload(Event.type_rel),
        )
        return self

    def by_sport(self, sport: str) -> "SessionQuery":
        self._query = self._query.where(Session.sport == sport)
        return self

    def by_venue(self, venue: str) -> "SessionQuery":
        self._query = self._query.where(Session.venue == venue)
        return self

    def by_zone(self, zone: str) -> "SessionQuery":
        self._query = self._query.join(Venue).where(Venue.zone == zone)
        return self

    def by_day(self, day: int) -> "SessionQuery":
        self._query = self._query.where(Session.day == day)
        return self

    def by_days(self, days: list[int]) -> "SessionQuery":
        self._query = self._query.where(Session.day.in_(days))
        return self

    def by_date(self, d: date) -> "SessionQuery":
        """Filter sessions that start on a given date."""
        start = datetime.combine(d, datetime.min.time())
        end = datetime.combine(d, datetime.max.time())
        self._query = self._query.where(Session.starts_at >= start).where(Session.starts_at <= end)
        return self

    def by_type(self, type_name: str) -> "SessionQuery":
        self._query = self._query.where(Session.type == type_name)
        return self

    def ticketed(self) -> "SessionQuery":
        self._query = self._query.where(Session.ticketed == True)
        return self

    def between(self, start: datetime, end: datetime) -> "SessionQuery":
        """Filter sessions starting between two datetimes."""
        self._query = self._query.where(Session.starts_at >= start).where(Session.starts_at <= end)
        return self

    def order_by_start(self, desc: bool = False) -> "SessionQuery":
        if desc:
            self._query = self._query.order_by(col(Session.starts_at).desc())
        else:
            self._query = self._query.order_by(Session.starts_at)
        return self

    def limit(self, n: int) -> "SessionQuery":
        self._query = self._query.limit(n)
        return self

    def fetch(self) -> list[Session]:
        return list(self._session.exec(self._query).all())

    def first(self) -> Session | None:
        return self._session.exec(self._query).first()

    def count(self) -> int:
        return len(self.fetch())


class EventQuery:
    """Fluent query builder for events."""

    def __init__(self, db_session: DBSession):
        self._session = db_session
        self._query = select(Event)

    def with_relations(self) -> "EventQuery":
        self._query = self._query.options(
            selectinload(Event.type_rel),
            selectinload(Event.session_rel).selectinload(Session.sport_rel),
            selectinload(Event.session_rel).selectinload(Session.venue_rel).selectinload(Venue.zone_rel),
        )
        return self

    def by_sex(self, sex: str) -> "EventQuery":
        self._query = self._query.where(Event.sex == sex)
        return self

    def by_type(self, type_name: str) -> "EventQuery":
        self._query = self._query.where(Event.type == type_name)
        return self

    def by_session(self, code: str) -> "EventQuery":
        self._query = self._query.where(Event.code == code)
        return self

    def by_sport(self, sport: str) -> "EventQuery":
        self._query = self._query.join(Session).where(Session.sport == sport)
        return self

    def by_day(self, day: int) -> "EventQuery":
        self._query = self._query.join(Session).where(Session.day == day)
        return self

    def finals(self) -> "EventQuery":
        return self.by_type("Final")

    def medals(self) -> "EventQuery":
        self._query = self._query.where(or_(Event.type == "Final", Event.type == "Bronze"))
        return self

    def search(self, term: str) -> "EventQuery":
        self._query = self._query.where(Event.description.contains(term))
        return self

    def order_by_number(self) -> "EventQuery":
        self._query = self._query.order_by(Event.event_number)
        return self

    def limit(self, n: int) -> "EventQuery":
        self._query = self._query.limit(n)
        return self

    def fetch(self) -> list[Event]:
        return list(self._session.exec(self._query).all())

    def first(self) -> Event | None:
        return self._session.exec(self._query).first()

    def count(self) -> int:
        return len(self.fetch())
