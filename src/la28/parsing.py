"""
Parse LA28 schedule from PDF or JSON into the database.

PDF parsing is the primary method, copied from read.py.
"""

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import json


from la28.database import Database
from la28.models import Zone, Venue, Sport, Session, Event, SportVenueLink, Day

CT = ZoneInfo("America/Chicago")
PT = ZoneInfo("America/Los_Angeles")


def _parse_time_cell(s: str):
    """Parse time cell from PDF, handling TBD and timezone markers."""
    lines = s.split("\n")
    is_ct = any("(CT)" in line for line in lines[1:])
    if lines[0] == "TBD":
        return "00:00", "23:59", is_ct
    return lines[0], None, is_ct


def _parse_event_type(line: str, session_type: str, is_single: bool) -> str:
    """Determine event type from description line."""
    if is_single:
        return session_type.replace('\n', ' ')

    if 'Final' in line or 'Gold Medal' in line:
        if any(x in line for x in ["Final B", "Final C", "Final D"]):
            return "Preliminary"
        return "Final"
    if 'Bronze' in line or 'Bronze Medal' in line:
        return "Bronze"
    if 'Semifinal' in line:
        return "Semifinal"
    if 'Quarterfinal' in line:
        return "Quarterfinal"
    if 'Preliminary' in line or 'Qualification' in line or 'Heats' in line or 'Pool' in line:
        return "Preliminary"
    if "Repechage" in line:
        return "Repechage"
    return "N/A"


def _parse_sex(line: str) -> str:
    """Determine sex category from description line."""
    if 'and/or' in line:
        return 'and/or'
    if 'or' in line:
        return 'or'
    if 'Men' in line and 'Women' not in line:
        return 'Men'
    if 'Women' in line and 'Men' not in line:
        return 'Women'
    if 'Mixed' in line:
        return 'Mixed'
    return 'and/or'




def load_from_json(db: Database, src: str | Path) -> dict:
    """
    Load schedule directly from LA28 PDF into database.

    This is the primary loading method.
    """
    with Path(src).open(encoding="utf-8") as f:
        rows = json.load(f)

    stats = {"days": set(), "zones": set(), "venues": set(), "sports": set(), "sessions": 0, "events": 0}

    with db.session() as s:
        for row in rows:
            # Parse date and times
            date = datetime.strptime(row["Date"] + " 2028", "%A, %B %d %Y").date()
            start_raw, end_override, is_ct = _parse_time_cell(row["Start Time"])
            end_raw, _, _ = _parse_time_cell(row["End Time"])
            tz = CT if is_ct else PT

            if end_override:
                start_time = datetime.strptime(f"{date} {start_raw}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
                end_time = datetime.strptime(f"{date} {end_override}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
            else:
                start_time = datetime.strptime(f"{date} {start_raw}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
                end_time = datetime.strptime(f"{date} {end_raw}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)

            # Extract fields
            zone_name = row["Zone"]
            venue_name = row["Venue"]
            sport_name = row["Sport"].replace('\n', ' ')
            session_type = row["Session Type"].replace('\n', ' ')

            # Create zone if needed
            if zone_name not in stats["zones"]:
                if not s.get(Zone, zone_name):
                    s.add(Zone(zone=zone_name))
                stats["zones"].add(zone_name)

            # Create venue if needed
            if venue_name not in stats["venues"]:
                if not s.get(Venue, venue_name):
                    s.add(Venue(venue=venue_name, zone=zone_name, in_okc=is_ct))
                stats["venues"].add(venue_name)

            # Create sport if needed
            if sport_name not in stats["sports"]:
                if not s.get(Sport, sport_name):
                    s.add(Sport(sport=sport_name))
                stats["sports"].add(sport_name)

            # Create day if needed
            day_num = int(row["Games Day"])
            if day_num not in stats["days"]:
                if not s.get(Day, day_num):
                    s.add(Day(day=day_num))
                stats["days"].add(day_num)

            # Link sport to venue
            from sqlmodel import select
            existing_link = s.exec(
                select(SportVenueLink)
                .where(SportVenueLink.sport == sport_name)
                .where(SportVenueLink.venue == venue_name)
            ).first()
            if not existing_link:
                s.add(SportVenueLink(sport=sport_name, venue=venue_name))

            # Parse session description for events
            desc = row["Session Description"]
            desc_lines = desc.split("\n")
            not_ticketed = desc_lines[0] == "Not Ticketed"
            if not_ticketed:
                desc_lines = desc_lines[1:]

            # Create session
            session = Session(
                code=row["Session Code"],
                day=int(row["Games Day"]),
                sport=sport_name,
                venue=venue_name,
                type=session_type,
                starts_at=start_time,
                ends_at=end_time,
                timezone=tz.key,
                ticketed=not not_ticketed,
            )
            s.add(session)
            stats["sessions"] += 1

            # Create events
            for i, line in enumerate(desc_lines):
                event = Event(
                    code=row["Session Code"],
                    sex=_parse_sex(line),
                    description=line,
                    type=_parse_event_type(line, session_type, len(desc_lines) == 1),
                    order_in_session=i + 1,
                    total_in_session=len(desc_lines),
                )
                s.add(event)
                stats["events"] += 1

        s.commit()

    # Second pass: compute session and event numbering
    _compute_numbering(db)

    return {
        "days": len(stats["days"]),
        "zones": len(stats["zones"]),
        "venues": len(stats["venues"]),
        "sports": len(stats["sports"]),
        "sessions": stats["sessions"],
        "events": stats["events"],
    }


def _compute_numbering(db: Database) -> None:
    """Compute session and event numbering after initial load."""
    from sqlmodel import select
    from sqlalchemy.orm import selectinload

    with db.session() as s:
        # Get all sessions ordered by start time
        sessions = s.exec(
            select(Session)
            .options(selectinload(Session.events))
            .order_by(Session.starts_at, Session.code)
        ).all()

        total_sessions = len(sessions)
        sport_session_counts: dict[str, int] = {}
        sport_session_totals: dict[str, int] = {}

        # Count total sessions per sport
        for sess in sessions:
            sport_session_totals[sess.sport] = sport_session_totals.get(sess.sport, 0) + 1

        # Number sessions
        for i, sess in enumerate(sessions, 1):
            sess.session_number = i
            sess.total_sessions = total_sessions
            sport_session_counts[sess.sport] = sport_session_counts.get(sess.sport, 0) + 1
            sess.sport_session_number = sport_session_counts[sess.sport]
            sess.total_sport_sessions = sport_session_totals[sess.sport]

        s.commit()

        # Get all events ordered by session start time, then order in session
        events = s.exec(
            select(Event)
            .join(Session)
            .order_by(Session.starts_at, Session.code, Event.order_in_session)
        ).all()

        total_events = len(events)
        sport_event_counts: dict[str, int] = {}
        sport_event_totals: dict[str, int] = {}

        # Count total events per sport (need to join with session)
        for evt in events:
            sess = s.get(Session, evt.code)
            if sess:
                sport_event_totals[sess.sport] = sport_event_totals.get(sess.sport, 0) + 1

        # Number events
        for i, evt in enumerate(events, 1):
            evt.event_number = i
            evt.total_events = total_events
            sess = s.get(Session, evt.code)
            if sess:
                sport_event_counts[sess.sport] = sport_event_counts.get(sess.sport, 0) + 1
                evt.sport_event_number = sport_event_counts[sess.sport]
                evt.total_sport_events = sport_event_totals[sess.sport]

        s.commit()
