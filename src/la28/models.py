"""
SQLModel-based models for LA28 Olympics schedule database.

Uses natural keys (names) instead of surrogate IDs.
Uses proper datetime types, not strings.
"""

from datetime import datetime, timedelta
from typing import Literal

from sqlmodel import SQLModel, Field, Relationship

SexType = Literal['Men', 'Women', 'Mixed', 'and/or', 'or']


# =============================================================================
# Junction Table
# =============================================================================

class SportVenueLink(SQLModel, table=True):
    __tablename__ = "sport_venue"

    sport: str = Field(foreign_key="sport.sport", primary_key=True)
    venue: str = Field(foreign_key="venue.venue", primary_key=True)
    is_primary: bool = Field(default=False)


# =============================================================================
# Lookup / Reference Tables
# =============================================================================

class Zone(SQLModel, table=True):
    __tablename__ = "zone"

    zone: str = Field(primary_key=True)
    description: str | None = None

    venues: list["Venue"] = Relationship(back_populates="zone_rel")



class Venue(SQLModel, table=True):
    __tablename__ = "venue"
    venue: str  = Field(primary_key=True)
    zone: str = Field(foreign_key="zone.zone")
    address: str | None = None
    capacity: int | None = None
    latitude: float | None = None
    longitude: float | None = None
    in_okc: bool = False
    zone_rel: Zone | None = Relationship(back_populates="venues")
    sports: list["Sport"] = Relationship(back_populates="venues", link_model=SportVenueLink)
    sessions: list["Session"] = Relationship(back_populates="venue_rel")


class Sport(SQLModel, table=True):
    __tablename__ = "sport"

    sport: str = Field(primary_key=True)
    description: str | None = None
    icon: str | None = None

    venues: list[Venue] = Relationship(back_populates="sports", link_model=SportVenueLink)
    sessions: list["Session"] = Relationship(back_populates="sport_rel")


class EventType(SQLModel, table=True):
    __tablename__ = "event_type"


    type: str = Field(primary_key=True)
    rank: int = Field(default=0)

    sessions: list["Session"] = Relationship(back_populates="type_rel")
    events: list["Event"] = Relationship(back_populates="type_rel")


class Day(SQLModel, table=True):
    __tablename__ = "days"
    day: int = Field(primary_key=True)

# =============================================================================
# Core Tables
# =============================================================================


class Session(SQLModel, table=True):
    __tablename__ = "session"

    code: str = Field(primary_key=True)
    day: int = Field(foreign_key="days.day")
    sport: str = Field(foreign_key="sport.sport")
    venue: str = Field(foreign_key="venue.venue")
    type: str | None = Field(foreign_key="event_type.type")
    starts_at: datetime
    ends_at: datetime
    timezone: str
    ticketed: bool = True
    session_number: int | None = None
    total_sessions: int | None = None
    sport_session_number: int | None = None
    total_sport_sessions: int | None = None
    sport_rel: Sport | None = Relationship(back_populates="sessions")
    venue_rel: Venue | None = Relationship(back_populates="sessions")
    type_rel: EventType | None = Relationship(back_populates="sessions")
    events: list["Event"] = Relationship(back_populates="session_rel")

    @property
    def duration(self) -> timedelta:
        return self.ends_at - self.starts_at

    @property
    def duration_minutes(self) -> int:
        return int(self.duration.total_seconds() // 60)



class Event(SQLModel, table=True):
    __tablename__ = "event"

    id: int | None = Field(default=None, primary_key=True)
    code: str = Field(foreign_key="session.code", index=True)
    sex: str = Field(index=True)
    description: str
    type: str | None = Field(default=None, foreign_key="event_type.type", index=True)
    order_in_session: int = Field(default=1)
    total_in_session: int = Field(default=1)
    event_number: int | None = None
    total_events: int | None = None
    sport_event_number: int | None = None
    total_sport_events: int | None = None
    session_rel: Session | None = Relationship(back_populates="events")
    type_rel: EventType | None = Relationship(back_populates="events")


# =============================================================================
# Seed Data (created lazily, not at import time)
# =============================================================================

def get_default_event_types() -> list[EventType]:
    return [
        EventType(type="Final", rank=1),
        EventType(type="Bronze", rank=2),
        EventType(type="Semifinal", rank=3),
        EventType(type="Quarterfinal", rank=4),
        EventType(type="Repechage", rank=5),
        EventType(type="Preliminary", rank=6),
        EventType(type="N/A", rank=99),
    ]


# =============================================================================
# Schedule View (denormalized join of events, sessions, venues, zones, sports)
# =============================================================================

class ScheduleView(SQLModel):
    """Flattened view joining Event, Session, Venue, Zone, Sport, and EventType."""

    # Event fields
    event_id: int
    event_sex: str
    event_description: str
    event_type: str | None
    order_in_session: int
    total_in_session: int
    event_number: int | None
    total_events: int | None

    # Session fields
    session_code: str
    day: int
    starts_at: datetime
    ends_at: datetime
    timezone: str
    ticketed: bool
    session_type: str | None
    session_number: int | None
    total_sessions: int | None

    # Sport fields
    sport: str

    # Venue fields
    venue: str
    venue_address: str | None
    venue_capacity: int | None
    venue_latitude: float | None
    venue_longitude: float | None
    in_okc: bool

    # Zone fields
    zone: str
    zone_description: str | None

    # EventType rank (for sorting)
    event_type_rank: int | None

    # Map links (computed)
    google_maps_url: str | None
    waze_url: str | None
    apple_maps_url: str | None
    osm_url: str | None