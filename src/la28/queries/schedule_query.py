"""
Schedule view query - joins events, sessions, venues, zones, sports, and event types.
"""

from datetime import datetime, date

from sqlmodel import Session as DBSession, select, col
from sqlalchemy import and_, case, literal_column, func

from la28.models import (
    Zone, Venue, Sport, EventType, Session, Event, ScheduleView
)


class ScheduleQuery:
    """Fluent query builder for the schedule view (Event + Session + Venue + Zone + Sport)."""

    def __init__(self, db_session: DBSession):
        self._session = db_session
        # Computed map URL expressions
        google_maps_url = case(
            (
                and_(Venue.latitude.isnot(None), Venue.longitude.isnot(None)),
                func.concat(
                    'https://www.google.com/maps/search/?api=1&query=',
                    Venue.latitude, ',', Venue.longitude
                )
            ),
            else_=None
        ).label("google_maps_url")

        waze_url = case(
            (
                and_(Venue.latitude.isnot(None), Venue.longitude.isnot(None)),
                func.concat(
                    'https://waze.com/ul?ll=',
                    Venue.latitude, ',', Venue.longitude, '&navigate=yes'
                )
            ),
            else_=None
        ).label("waze_url")

        apple_maps_url = case(
            (
                and_(Venue.latitude.isnot(None), Venue.longitude.isnot(None)),
                func.concat(
                    'https://maps.apple.com/?ll=',
                    Venue.latitude, ',', Venue.longitude,
                    '&q=', func.replace(Venue.venue, ' ', '+')
                )
            ),
            else_=None
        ).label("apple_maps_url")

        osm_url = case(
            (
                and_(Venue.latitude.isnot(None), Venue.longitude.isnot(None)),
                func.concat(
                    'https://www.openstreetmap.org/?mlat=',
                    Venue.latitude, '&mlon=', Venue.longitude, '&zoom=17'
                )
            ),
            else_=None
        ).label("osm_url")

        self._query = (
            select(
                Event.id.label("event_id"),
                Event.sex.label("event_sex"),
                Event.description.label("event_description"),
                Event.type.label("event_type"),
                Event.order_in_session,
                Event.total_in_session,
                Event.event_number,
                Event.total_events,
                Session.code.label("session_code"),
                Session.day,
                Session.starts_at,
                Session.ends_at,
                Session.timezone,
                Session.ticketed,
                Session.type.label("session_type"),
                Session.session_number,
                Session.total_sessions,
                Sport.sport,
                Venue.venue,
                Venue.address.label("venue_address"),
                Venue.capacity.label("venue_capacity"),
                Venue.latitude.label("venue_latitude"),
                Venue.longitude.label("venue_longitude"),
                Venue.in_okc,
                Zone.zone,
                Zone.description.label("zone_description"),
                EventType.rank.label("event_type_rank"),
                google_maps_url,
                waze_url,
                apple_maps_url,
                osm_url,
            )
            .select_from(Event)
            .join(Session, Session.code == Event.code)
            .join(Venue, Venue.venue == Session.venue)
            .join(Zone, Zone.zone == Venue.zone)
            .join(Sport, Sport.sport == Session.sport)
            .outerjoin(EventType, EventType.type == Event.type)
        )

    def by_sport(self, sport: str) -> "ScheduleQuery":
        self._query = self._query.where(Sport.sport == sport)
        return self

    def by_sports(self, sports: list[str]) -> "ScheduleQuery":
        self._query = self._query.where(Sport.sport.in_(sports))
        return self

    def by_venue(self, venue: str) -> "ScheduleQuery":
        self._query = self._query.where(Venue.venue == venue)
        return self

    def by_venues(self, venues: list[str]) -> "ScheduleQuery":
        self._query = self._query.where(Venue.venue.in_(venues))
        return self

    def by_zone(self, zone: str) -> "ScheduleQuery":
        self._query = self._query.where(Zone.zone == zone)
        return self

    def by_zones(self, zones: list[str]) -> "ScheduleQuery":
        self._query = self._query.where(Zone.zone.in_(zones))
        return self

    def by_day(self, day: int) -> "ScheduleQuery":
        self._query = self._query.where(Session.day == day)
        return self

    def by_days(self, days: list[int]) -> "ScheduleQuery":
        self._query = self._query.where(Session.day.in_(days))
        return self

    def by_date(self, d: date) -> "ScheduleQuery":
        """Filter events that occur on a given date."""
        start = datetime.combine(d, datetime.min.time())
        end = datetime.combine(d, datetime.max.time())
        self._query = self._query.where(
            and_(Session.starts_at >= start, Session.starts_at <= end)
        )
        return self

    def by_event_type(self, type_name: str) -> "ScheduleQuery":
        self._query = self._query.where(Event.type == type_name)
        return self

    def by_event_types(self, type_names: list[str]) -> "ScheduleQuery":
        self._query = self._query.where(Event.type.in_(type_names))
        return self

    def by_sex(self, sex: str) -> "ScheduleQuery":
        self._query = self._query.where(Event.sex == sex)
        return self

    def ticketed(self) -> "ScheduleQuery":
        self._query = self._query.where(Session.ticketed == True)
        return self

    def in_okc(self) -> "ScheduleQuery":
        self._query = self._query.where(Venue.in_okc == True)
        return self

    def finals_only(self) -> "ScheduleQuery":
        self._query = self._query.where(Event.type == "Final")
        return self

    def medal_events(self) -> "ScheduleQuery":
        """Filter to finals and bronze medal events."""
        self._query = self._query.where(Event.type.in_(["Final", "Bronze"]))
        return self

    def between(self, start: datetime, end: datetime) -> "ScheduleQuery":
        """Filter events starting between two datetimes."""
        self._query = self._query.where(
            and_(Session.starts_at >= start, Session.starts_at <= end)
        )
        return self

    def order_by_start(self, desc: bool = False) -> "ScheduleQuery":
        if desc:
            self._query = self._query.order_by(
                col(Session.starts_at).desc(),
                col(Event.order_in_session)
            )
        else:
            self._query = self._query.order_by(
                Session.starts_at,
                Event.order_in_session
            )
        return self

    def order_by_event_type(self) -> "ScheduleQuery":
        """Order by event type rank (Finals first, then Bronze, Semifinal, etc.)."""
        self._query = self._query.order_by(
            col(EventType.rank),
            Session.starts_at,
            Event.order_in_session
        )
        return self

    def limit(self, n: int) -> "ScheduleQuery":
        self._query = self._query.limit(n)
        return self

    def offset(self, n: int) -> "ScheduleQuery":
        self._query = self._query.offset(n)
        return self

    def fetch(self) -> list[ScheduleView]:
        """Execute query and return list of ScheduleView objects."""
        rows = self._session.exec(self._query).all()
        return [ScheduleView.model_validate(dict(row._mapping)) for row in rows]

    def fetch_raw(self) -> list:
        """Execute query and return raw row tuples."""
        return list(self._session.exec(self._query).all())

    def first(self) -> ScheduleView | None:
        result = self._session.exec(self._query.limit(1)).first()
        if result:
            return ScheduleView.model_validate(dict(result._mapping))
        return None

    def count(self) -> int:
        return len(self.fetch_raw())
