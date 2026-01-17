import csv
import json
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Literal, Any
from zoneinfo import ZoneInfo
from io import BytesIO

import requests
import pdfplumber




CT = ZoneInfo("America/Chicago")
PT = ZoneInfo("America/Los_Angeles")


def _parse_time_cell(s: str):
    lines = s.split("\n")
    is_ct = any("(CT)" in line for line in lines[1:])

    if lines[0] == "TBD":
        return "00:00", "23:59", is_ct

    return lines[0], None, is_ct



def _tz(s: str):
    return CT if "\nOKC Local Time (CT)" in s else PT

def _dt(d: datetime) -> str:
    return d.isoformat()

def _dt_utc(d: datetime) -> str:
    return d.astimezone(timezone.utc).isoformat()

class SessionType:
    FINAL = "Final"
    SEMIFINAL = "Semifinal"
    QUARTERFINAL = "Quarterfinal"
    PRELIMINARY = "Preliminary"
    BRONZE = "Bronze"
    NA = "N/A"

SessionTypeHint = Literal[
    "Final",
    "Semifinal",
    "Quarterfinal",
    "Preliminary",
        "Repechage",
    "Bronze",
    "N/A",
]

@dataclass
class SessionEvent:
    sex: Literal['Men', 'Women', 'Mixed', 'and/or', 'or']
    description: str
    type: SessionTypeHint
    order: int
    total: int
    session: Any = None

    event_number: int | None = None
    total_events: int | None = None
    sport_event_number: int | None = None
    total_sport_events: int | None = None

    def to_json(self, include_session_info: bool = False, include_computable: bool = True) -> dict[str, Any]:
        return {
            **(self.session.to_json(include_events=False, include_computable=include_computable) if include_session_info else {}),
            "sex": self.sex,
            "description": self.description,
            "type": self.type,
            **({
                "order": self.order,
                "total": self.total,
                "sport_event_number": self.sport_event_number,
                "total_sport_events": self.total_sport_events,
                "event_number": self.event_number,
                "total_events": self.total_events,
            } if include_computable else {}),
        }

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> "SessionEvent":
        return cls(
            sex=d["sex"],
            description=d["description"],
            type=d["type"],
            order=d["order"],
            total=d["total"],
        )

@dataclass
class Session:
    sport: str
    venue: str
    zone: str
    start_time: datetime
    end_time: datetime
    type: SessionTypeHint
    events: list[SessionEvent]
    ticketed: bool
    in_okc: bool

    session_number: int | None = None
    total_sessions: int | None = None
    sport_session_number: int | None = None
    total_sport_sessions: int | None = None

    @property
    def date(self) -> date:
        return self.start_time.date()

    @property
    def duration(self) -> timedelta:
        return self.end_time - self.start_time

    @classmethod
    def from_dict(cls, d):
        date = datetime.strptime(d["Date"] + " 2028", "%A, %B %d %Y").date()

        start_raw, start_end_override, is_ct = _parse_time_cell(d["Start Time"])
        end_raw, _, _ = _parse_time_cell(d["End Time"])

        tz = CT if is_ct else PT

        if start_end_override:
            start_time = datetime.strptime(
                f"{date} {start_raw}", "%Y-%m-%d %H:%M"
            ).replace(tzinfo=tz)
            end_time = datetime.strptime(
                f"{date} {start_end_override}", "%Y-%m-%d %H:%M"
            ).replace(tzinfo=tz)
        else:
            start_time = datetime.strptime(
                f"{date} {start_raw}", "%Y-%m-%d %H:%M"
            ).replace(tzinfo=tz)
            end_time = datetime.strptime(
                f"{date} {end_raw}", "%Y-%m-%d %H:%M"
            ).replace(tzinfo=tz)

        desc = d["Session Description"]
        desc_lines = desc.split("\n")
        not_ticketed = desc_lines[0] == "Not Ticketed"
        if not_ticketed:
            desc_lines = desc_lines[1:]
        events = []
        for i, line in enumerate(desc_lines):
            t = 'and/or'
            if 'and/or' in line:
                t = 'and/or'
            elif 'or' in line:
                t = 'or'
            elif 'Men' in line and 'Women' not in line:
                t = 'Men'
            elif 'Women' in line and 'Men' not in line:
                t = 'Women'
            elif 'Mixed' in line:
                t = 'Mixed'
            else:
                t = 'and/or'

            if len(desc_lines) == 1:
                ty = d["Session Type"].replace('\n', ' ')
            else:
                if 'Final' in line or 'Gold Medal' in line and not any(x in line for x in ["Final B", "Final C", "Final D"]):
                    ty = "Final"
                elif 'Bronze' in line or 'Bronze Medal' in line:
                    ty = "Bronze"
                elif 'Semifinal' in line:
                    ty = "Semifinal"
                elif 'Quarterfinal' in line:
                    ty = "Quarterfinal"
                elif 'Preliminary' in line or 'Qualification' in line or 'Heats' in line or 'Pool' in line:
                    ty = "Preliminary"
                elif "Repechage" in line:
                    ty = "Repechage"
                else:
                    ty = "N/A"
            events.append(SessionEvent(sex=t, description=line, type=ty, order=i + 1, total=len(desc_lines)))

        s = cls(
            sport=d["Sport"].replace('\n', ' '),
            venue=d["Venue"],
            zone=d["Zone"],
            start_time=start_time,
            end_time=end_time,
            type=d["Session Type"].replace('\n', ' '),
            ticketed=not not_ticketed,
            events=events,
            in_okc=is_ct
        )
        for e in s.events:
            e.session = s

        return s

    def to_json(self, *, include_events: bool = True, include_utc: bool = True, include_computable: bool = True) -> dict[str, Any]:
        tz = self.start_time.tzinfo
        out = {
            "sport": self.sport,
            "venue": self.venue,
            "zone": self.zone,
            "ticketed": self.ticketed,
            "type": self.type,
            "startsAt": _dt(self.start_time),
            "endsAt": _dt(self.end_time),
            "timezone": (
    tz.key if hasattr(tz, "key")
    else tz.tzname(self.start_time) if tz
    else None
),
            "durationMinutes": int(self.duration.total_seconds() // 60),
            "inOKC": self.in_okc,


        }
        if include_computable:
            out.update({
                "sportSessionNumber": self.sport_session_number,
                "totalSportSessions": self.total_sport_sessions,
                "sessionNumber": self.session_number,
                "totalSessions": self.total_sessions,
            })

        if include_utc:
            out["startsAtUtc"] = _dt_utc(self.start_time)
            out["endsAtUtc"] = _dt_utc(self.end_time)

        if include_events:
            out["events"] = [e.to_json(include_session_info=False, include_computable=include_computable) for e in self.events]


        return out

    @classmethod
    def from_json(cls, d):
        s = cls(
            sport=d["sport"],
            venue=d["venue"],
            zone=d["zone"],
            type=d["type"],
            ticketed=d["ticketed"],
            start_time=datetime.fromisoformat(d["startsAt"]),
            end_time=datetime.fromisoformat(d["endsAt"]),
            events=[SessionEvent.from_json(x) for x in d["events"]],
            in_okc=d["inOKC"],
            sport_session_number=d.get("sportSessionNumber"),
            total_sport_sessions=d.get("totalSportSessions"),
            session_number=d.get("sessionNumber"),
            total_sessions=d.get("totalSessions"),
        )
        for e in s.events:
            e.session = s
        return s

    def __iter__(self):
        return iter(self.events)


def read_pdf_tables(url) -> list[list[str]]:
    # download PDF
    pdf_bytes = requests.get(url).content

    all_tables = []
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            all_tables.append(table)
    return all_tables

def read_pdf_session_rows(url) -> list[dict[str, str]]:
    tables = read_pdf_tables(url=url)

    # extract text
    header = tables[0][1]
    sessions = []
    for table in tables:
        for session in table[2:]:
            s = {header[i]: session[i] for i in range(len(header))}
            sessions.append(s)
    return sessions

def read_pdf_text(url) -> list[Session]:
    all_sessions = read_pdf_session_rows(url=url)
    sessions = [Session.from_dict(session) for session in all_sessions]

    return sessions


class Schedule(list):
    @classmethod
    def fetch(cls, url):
        x = cls(read_pdf_text(url=url))
        x.recount()
        return x

    def recount(self):
        total_sessions = len(self)
        events = [e for session in self for e in session.events]
        total_events = len(events)
        sorted_sessions = sorted(self, key=lambda s: s.start_time)
        sports = set(s.sport for s in sorted_sessions)
        sport_event_counts = {k: 0 for k in sports}
        sport_session_counts = {k: 0 for k in sports}
        en = 0
        for i, session in enumerate(sorted_sessions):
            session.total_sessions = total_sessions
            session.session_number = i + 1
            sport_session_counts[session.sport] += 1
            n = sport_session_counts[session.sport]
            session.sport_session_number = n
            for event in session.events:
                en += 1
                event.event_number = en
                event.total_events = total_events
                sport_event_counts[session.sport] += 1
                n = sport_event_counts[session.sport]
                event.sport_event_number = n
        for session in sorted_sessions:
            session.total_sport_sessions = sport_session_counts[session.sport]
            for event in session.events:
                event.total_sport_events = sport_event_counts[session.sport]

    def parsed(self) -> list[dict[str, Any]]:
        return [session.to_json() for session in self]

    def dumps(self, indent=2) -> str:
        return json.dumps(self.parsed(), indent=indent)

    def dump(self, path: str | Path, indent=2) -> None:
        with Path(path).open("w", encoding="utf-8") as f:
            f.write(self.dumps(indent=indent))

    @classmethod
    def from_json(cls, path: str | Path) -> "Schedule":
        if isinstance(path, str) and path.strip().startswith("[") and path.strip().endswith("]"):
            sessions = json.loads(path)
        else:
            with Path(path).open("r", encoding="utf-8") as f:
                sessions = json.loads(f.read())
        return cls([Session.from_json(session) for session in sessions])

    load = from_json
    loads = from_json

    @property
    def sessions(self) -> list[Session]:
        return self

    @property
    def events(self) -> list[SessionEvent]:
        return [e for session in self for e in session.events]

    @property
    def sports(self) -> list[str]:
        return list(set(x.sport for x in self))

    @property
    def venues(self) -> list[str]:
        return list(set(x.venue for x in self))

    @property
    def zones(self) -> list[str]:
        return list(set(x.zone for x in self))

    @property
    def types(self) -> list[str]:
        return list(set(x.types for x in self))

    @property
    def sexes(self) -> list[str]:
        return list(set(x.sex for x in self))

    @property
    def count(self):
        return len(self)

    @property
    def start_time(self) -> datetime:
        return min(e.start_time for e in self)

    @property
    def end_time(self) -> datetime:
        return max(e.end_time for e in self)

    @property
    def duration(self) -> timedelta:
        return self.end_time - self.start_time

    @property
    def num_sports(self) -> int:
        return len(self.sports)

    @property
    def num_venues(self) -> int:
        return len(self.venues)

    @property
    def num_events(self) -> int:
        return len(self.events)

def export_sessions_json(schedule: Schedule, path: str | Path) -> None:
    schedule.dump(path)

def export_events_json(schedule: Schedule, path: str | Path) -> None:
    with Path(path).open('w') as f:
        json.dump([e.to_json(True) for session in schedule for e in session.events], f, indent=2)

def export_events_csv(schedule: Schedule, path: str | Path = "events_flat.csv"):
    # one row per SessionEvent, with Session columns copied onto each row
    fieldnames = [
        # session columns
        "date",
        "sport",
        "venue",
        "zone",
        "ticketed",
        "inOKC",
        "sessionType",
        "startsAt",
        "endsAt",
        "startsAtUtc",
        "endsAtUtc",
        "durationMinutes",
        "sessionNumber",
        "totalSessions",
        "sportSessionNumber",
        "totalSportSessions",
        # event columns
        "eventNumber",
        "totalEvents",
        "sportEventNumber",
        "totalSportEvents",
        "sex",
        "eventType",
        "description",
        "eventOrder",
        "eventTotalInSession",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        for s in schedule.sessions:
            s_json = s.to_json(include_events=False, include_utc=True, include_computable=True)
            for e in s.events:
                e_json = e.to_json(include_session_info=False, include_computable=True)
                w.writerow({
                    "date": s.start_time.date().isoformat(),
                    "sport": s_json["sport"],
                    "venue": s_json["venue"],
                    "zone": s_json["zone"],
                    "ticketed": s_json["ticketed"],
                    "inOKC": s_json["inOKC"],
                    "sessionType": s_json["type"],
                    "startsAt": s_json["startsAt"],
                    "endsAt": s_json["endsAt"],
                    "startsAtUtc": s_json.get("startsAtUtc"),
                    "endsAtUtc": s_json.get("endsAtUtc"),
                    "durationMinutes": s_json["durationMinutes"],
                    "sessionNumber": s_json.get("sessionNumber"),
                    "totalSessions": s_json.get("totalSessions"),
                    "sportSessionNumber": s_json.get("sportSessionNumber"),
                    "totalSportSessions": s_json.get("totalSportSessions"),
                    "eventNumber": e_json.get("event_number"),
                    "totalEvents": e_json.get("total_events"),
                    "sportEventNumber": e_json.get("sport_event_number"),
                    "totalSportEvents": e_json.get("total_sport_events"),
                    "sex": e_json["sex"],
                    "eventType": e_json["type"],
                    "description": e_json["description"],
                    "eventOrder": e_json.get("order"),
                    "eventTotalInSession": e_json.get("total"),
                })

def export_sessions_csv(schedule: Schedule, path: str = "sessions.csv"):
    import csv
    fieldnames = [
        "date","sport","venue","zone","ticketed","inOKC","sessionType",
        "startsAt","endsAt","startsAtUtc","endsAtUtc","durationMinutes",
        "sessionNumber","totalSessions","sportSessionNumber","totalSportSessions",
        "numEventsInSession",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for s in schedule.sessions:
            j = s.to_json(include_events=False, include_utc=True, include_computable=True)
            w.writerow({
                "date": s.start_time.date().isoformat(),
                "sport": j["sport"],
                "venue": j["venue"],
                "zone": j["zone"],
                "ticketed": j["ticketed"],
                "inOKC": j["inOKC"],
                "sessionType": j["type"],
                "startsAt": j["startsAt"],
                "endsAt": j["endsAt"],
                "startsAtUtc": j.get("startsAtUtc"),
                "endsAtUtc": j.get("endsAtUtc"),
                "durationMinutes": j["durationMinutes"],
                "sessionNumber": j.get("sessionNumber"),
                "totalSessions": j.get("totalSessions"),
                "sportSessionNumber": j.get("sportSessionNumber"),
                "totalSportSessions": j.get("totalSportSessions"),
                "numEventsInSession": len(s.events),
            })

if __name__ == "__main__":
    url="https://la28.org/content/dam/latwentyeight/competition-schedule-imagery/uploaded-nov-12-2025/LA28OlympicGamesCompetitionScheduleByEventV2Final.pdf"
    fresh = False
    # fresh = True
    s = x = schedule = Schedule.fetch(url) if fresh else Schedule.load('sessions.json')

    export_sessions_json(schedule, 'sessions.json')
    export_events_json(schedule, "events.json")
    export_events_csv(schedule, "events.csv")
    export_sessions_csv(schedule, "sesssions.csv")