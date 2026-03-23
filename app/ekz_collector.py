"""
EKZ Energy Data Collector for Home Assistant — v5.0

Modes:
  - daemon:   Run continuously, fetch daily 15-min data + refresh monthly history
  - oneshot:  Fetch once and exit
  - backfill: Fetch monthly daily data from BACKFILL_FROM to yesterday, then exit
"""

import calendar
import json
import logging
import os
import pathlib
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import paho.mqtt.client as mqtt
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("ekz_collector")
LOCAL_TZ = ZoneInfo("Europe/Zurich")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    ekz_username: str
    ekz_password: str
    mqtt_host: str
    ekz_installation_id: str = ""
    mqtt_port: int = 1883
    mqtt_username: str = ""
    mqtt_password: str = ""
    mqtt_topic_prefix: str = "ekz_energy"
    ha_discovery_prefix: str = "homeassistant"
    fetch_days_back: int = 2
    peak_hour_start: int = 7
    peak_hour_end: int = 20
    mode: str = "daemon"
    schedule_hours: float = 4.0
    backfill_from: str = "2025-01-01"
    api_delay_seconds: float = 2.0
    data_dir: str = "/data"
    ha_url: str = ""
    ha_token: str = ""

    @classmethod
    def from_env(cls) -> "Config":
        username = os.environ.get("EKZ_USERNAME", "")
        password = os.environ.get("EKZ_PASSWORD", "")
        mqtt_host = os.environ.get("MQTT_HOST", "")

        if not all([username, password, mqtt_host]):
            missing = [n for n, v in [("EKZ_USERNAME", username), ("EKZ_PASSWORD", password), ("MQTT_HOST", mqtt_host)] if not v]
            raise ValueError(f"Missing required env vars: {', '.join(missing)}")

        return cls(
            ekz_username=username, ekz_password=password, mqtt_host=mqtt_host,
            ekz_installation_id=os.environ.get("EKZ_INSTALLATION_ID", ""),
            mqtt_port=int(os.environ.get("MQTT_PORT", "1883")),
            mqtt_username=os.environ.get("MQTT_USERNAME", ""),
            mqtt_password=os.environ.get("MQTT_PASSWORD", ""),
            mqtt_topic_prefix=os.environ.get("MQTT_TOPIC_PREFIX", "ekz_energy"),
            ha_discovery_prefix=os.environ.get("HA_DISCOVERY_PREFIX", "homeassistant"),
            fetch_days_back=int(os.environ.get("FETCH_DAYS_BACK", "2")),
            peak_hour_start=int(os.environ.get("PEAK_HOUR_START", "7")),
            peak_hour_end=int(os.environ.get("PEAK_HOUR_END", "20")),
            mode=os.environ.get("MODE", "daemon"),
            schedule_hours=float(os.environ.get("SCHEDULE_HOURS", "4")),
            backfill_from=os.environ.get("BACKFILL_FROM", "2025-01-01"),
            api_delay_seconds=float(os.environ.get("API_DELAY_SECONDS", "2")),
            ha_url=os.environ.get("HA_URL", ""),
            ha_token=os.environ.get("HA_TOKEN", ""),
            data_dir=os.environ.get("DATA_DIR", "/data"),
        )


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class IntervalReading:
    timestamp: datetime
    kwh: float
    is_peak: bool


@dataclass(frozen=True)
class DailySummary:
    date: str
    total_kwh: float
    peak_kwh: float
    offpeak_kwh: float
    intervals: list[IntervalReading] = field(default_factory=list)


@dataclass
class DailyRecord:
    date: str
    ht_kwh: float
    nt_kwh: float
    total_kwh: float
    status: str  # "VALID", "NOT_AVAILABLE", etc.


# ---------------------------------------------------------------------------
# History cache
# ---------------------------------------------------------------------------

class HistoryCache:
    """Persistent JSON cache for monthly daily consumption data."""

    def __init__(self, data_dir: str) -> None:
        self.path = pathlib.Path(data_dir) / "history.json"
        self.data: dict[str, Any] = {}  # key: "YYYY-MM", value: month data
        self._load()

    def _load(self) -> None:
        if self.path.exists():
            try:
                self.data = json.loads(self.path.read_text(encoding="utf-8"))
                logger.info("Loaded history cache: %d months", len(self.data))
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("Failed to load cache: %s — starting fresh", e)
                self.data = {}
        else:
            logger.info("No history cache found — starting fresh")

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.debug("Saved history cache: %d months", len(self.data))

    def has_complete_month(self, year: int, month: int) -> bool:
        """Check if a past month is cached and complete (won't change)."""
        key = f"{year:04d}-{month:02d}"
        if key not in self.data:
            return False
        now = datetime.now()
        # Current month is never "complete" — always re-fetch
        if year == now.year and month == now.month:
            return False
        # Previous month: consider complete if we have data
        return bool(self.data[key].get("days"))

    def get_month(self, year: int, month: int) -> dict[str, Any] | None:
        key = f"{year:04d}-{month:02d}"
        return self.data.get(key)

    def set_month(self, year: int, month: int, days: list[dict[str, Any]]) -> None:
        key = f"{year:04d}-{month:02d}"
        ht_total = sum(d.get("ht_kwh", 0) for d in days)
        nt_total = sum(d.get("nt_kwh", 0) for d in days)
        self.data[key] = {
            "year": year,
            "month": month,
            "days": days,
            "ht_kwh": round(ht_total, 2),
            "nt_kwh": round(nt_total, 2),
            "total_kwh": round(ht_total + nt_total, 2),
            "fetched_at": datetime.now().isoformat(),
        }

    def get_all_daily_records(self) -> list[dict[str, Any]]:
        """Return all daily records sorted by date."""
        records: list[dict[str, Any]] = []
        for key in sorted(self.data.keys()):
            month_data = self.data[key]
            for day in month_data.get("days", []):
                records.append(day)
        return records

    def compute_yearly_totals(self, year: int) -> dict[str, float]:
        ht = 0.0
        nt = 0.0
        for key, month_data in self.data.items():
            if month_data.get("year") == year:
                ht += month_data.get("ht_kwh", 0)
                nt += month_data.get("nt_kwh", 0)
        return {"ht_kwh": round(ht, 2), "nt_kwh": round(nt, 2), "total_kwh": round(ht + nt, 2)}

    def compute_monthly_totals(self, year: int, month: int) -> dict[str, float]:
        key = f"{year:04d}-{month:02d}"
        m = self.data.get(key, {})
        return {
            "ht_kwh": m.get("ht_kwh", 0),
            "nt_kwh": m.get("nt_kwh", 0),
            "total_kwh": m.get("total_kwh", 0),
        }



# ---------------------------------------------------------------------------
# myEKZ API client
# ---------------------------------------------------------------------------

EKZ_LOGIN_URL = "https://my.ekz.ch/login"
EKZ_API_BASE = "https://my.ekz.ch/api/portal-services/consumption-view/v1"


def ekz_login(session: requests.Session, username: str, password: str) -> bool:
    logger.info("Logging in to myEKZ...")
    resp = session.get(EKZ_LOGIN_URL, timeout=30, allow_redirects=True)
    if resp.status_code != 200:
        logger.error("Failed to load login page: %d", resp.status_code)
        return False

    soup = BeautifulSoup(resp.text, "html.parser")
    form = soup.find("form", id="kc-form-login") or soup.find("form", attrs={"action": True}) or soup.find("form")
    if form is None:
        logger.error("Could not find login form")
        return False

    action_url = form.get("action", "")
    if not action_url:
        return False
    if action_url.startswith("/"):
        parsed = urlparse(resp.url)
        action_url = f"{parsed.scheme}://{parsed.netloc}{action_url}"

    resp = session.post(action_url, data={"username": username, "password": password}, timeout=30, allow_redirects=True)
    if "my.ekz.ch" in resp.url and resp.status_code == 200:
        logger.info("Login successful")
        return True

    if "login.ekz.ch" in resp.url:
        error_soup = BeautifulSoup(resp.text, "html.parser")
        el = error_soup.find(class_="alert-error") or error_soup.find(class_="kc-feedback-text")
        logger.error("Login failed: %s", el.get_text(strip=True) if el else "unknown")
    return False


def discover_installation_id(session: requests.Session) -> str | None:
    logger.info("Auto-discovering installation ID...")
    resp = session.get(f"{EKZ_API_BASE}/installation-selection-data?installationVariant=CONSUMPTION", timeout=30)
    if resp.status_code != 200:
        logger.error("Failed to fetch installation data: %d", resp.status_code)
        return None
    data = resp.json()
    for entry in data.get("eanl", []):
        if isinstance(entry, dict) and entry.get("sparte") == "ELECTRICITY" and entry.get("anlage"):
            logger.info("Discovered installation ID: %s", entry["anlage"])
            return str(entry["anlage"])
    for entry in data.get("contracts", []):
        if isinstance(entry, dict) and entry.get("anlage"):
            return str(entry["anlage"])
    logger.error("Could not find installation ID")
    return None


def api_fetch(session: requests.Session, url: str, delay: float) -> dict[str, Any] | list | None:
    """Fetch from API with rate limiting."""
    logger.debug("API GET: %s", url)
    time.sleep(delay)  # Rate limit: wait BEFORE the call
    resp = session.get(url, timeout=30)
    if resp.status_code == 401:
        logger.error("Unauthorized — session expired")
        return None
    if resp.status_code != 200:
        logger.error("API %d: %s", resp.status_code, resp.text[:300])
        return None
    return resp.json()


# ---------------------------------------------------------------------------
# 15-min interval fetch (existing daily detail)
# ---------------------------------------------------------------------------

def fetch_15min(session: requests.Session, installation_id: str, target_date: datetime, delay: float) -> dict[str, Any] | None:
    date_from = (target_date - timedelta(days=1)).strftime("%Y-%m-%d")
    date_to = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
    url = f"{EKZ_API_BASE}/consumption-data?installationId={installation_id}&from={date_from}&to={date_to}&type=PK_VERB_15MIN"
    logger.info("Fetching 15-min data for %s to %s", date_from, date_to)
    return api_fetch(session, url, delay)


def parse_15min(raw: Any, target_date: datetime, config: Config) -> DailySummary:
    empty = DailySummary(date=target_date.strftime("%Y-%m-%d"), total_kwh=0, peak_kwh=0, offpeak_kwh=0)
    if not isinstance(raw, dict):
        return empty

    def extract(series_key: str) -> tuple[float, list[IntervalReading]]:
        series = raw.get(series_key)
        if not isinstance(series, dict):
            return 0.0, []
        values = series.get("values", [])
        is_peak = series.get("tariffType", "").upper() == "HT"
        total = 0.0
        intervals: list[IntervalReading] = []
        for item in values:
            if not isinstance(item, dict) or item.get("status") == "NOT_AVAILABLE":
                continue
            ts = None
            if item.get("date") and item.get("time"):
                try:
                    ts = datetime.strptime(f"{item['date']} {item['time']}", "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue
            if ts is None:
                continue
            kwh = float(item.get("value", 0) or 0)
            total += kwh
            intervals.append(IntervalReading(timestamp=ts, kwh=kwh, is_peak=is_peak))
        return total, intervals

    pk, pi = extract("seriesHt")
    ok, oi = extract("seriesNt")
    if not pi and not oi:
        ck, ci = extract("series")
        if ci:
            for iv in ci:
                wd = iv.timestamp.weekday() < 5
                if wd and config.peak_hour_start <= iv.timestamp.hour < config.peak_hour_end:
                    pk += iv.kwh
                    pi.append(IntervalReading(timestamp=iv.timestamp, kwh=iv.kwh, is_peak=True))
                else:
                    ok += iv.kwh
                    oi.append(IntervalReading(timestamp=iv.timestamp, kwh=iv.kwh, is_peak=False))

    intervals = sorted(pi + oi, key=lambda r: r.timestamp)
    total = pk + ok
    if not intervals:
        return empty

    return DailySummary(
        date=target_date.strftime("%Y-%m-%d"), total_kwh=round(total, 4), peak_kwh=round(pk, 4), offpeak_kwh=round(ok, 4),
        intervals=intervals,
    )


# ---------------------------------------------------------------------------
# Monthly daily fetch (for backfill / history)
# ---------------------------------------------------------------------------

def fetch_month_daily(session: requests.Session, installation_id: str, year: int, month: int, delay: float) -> dict[str, Any] | None:
    last_day = calendar.monthrange(year, month)[1]
    date_from = f"{year:04d}-{month:02d}-01"
    date_to = f"{year:04d}-{month:02d}-{last_day:02d}"
    url = f"{EKZ_API_BASE}/consumption-data?installationId={installation_id}&from={date_from}&to={date_to}&type=PK_VERB_TAG_METER"
    logger.info("Fetching monthly daily data: %s to %s", date_from, date_to)
    return api_fetch(session, url, delay)


def parse_month_daily(raw: Any, year: int, month: int) -> list[dict[str, Any]]:
    """Parse monthly daily API response into a list of daily records."""
    if not isinstance(raw, dict):
        logger.warning("parse_month_daily %04d-%02d: response is not a dict (type=%s)", year, month, type(raw).__name__)
        return []

    month_prefix = f"{year:04d}-{month:02d}"
    # Log available keys for debugging
    logger.debug("parse_month_daily %s: response keys=%s", month_prefix, list(raw.keys()))

    def extract_series(key: str) -> dict[str, float]:
        """Returns {date_str: kwh} for a series."""
        series = raw.get(key)
        if not isinstance(series, dict):
            return {}
        result: dict[str, float] = {}
        for item in series.get("values", []):
            if not isinstance(item, dict):
                continue
            date = item.get("date", "")
            if not date.startswith(month_prefix):
                continue
            if item.get("status") == "NOT_AVAILABLE":
                continue
            kwh = float(item.get("value", 0) or 0)
            result[date] = kwh
        return result

    ht_data = extract_series("seriesHt")
    nt_data = extract_series("seriesNt")

    # Fallback: if API returns combined "series" instead of HT/NT split,
    # assign all consumption to HT (matches parse_15min behaviour).
    if not ht_data and not nt_data:
        combined = extract_series("series")
        if combined:
            ht_data = combined
            logger.info("Daily data for %s: using combined 'series' (no HT/NT split)", month_prefix)

    if not ht_data and not nt_data:
        logger.warning("parse_month_daily %s: no data in seriesHt/seriesNt/series — check API response", month_prefix)

    all_dates = sorted(set(list(ht_data.keys()) + list(nt_data.keys())))
    records: list[dict[str, Any]] = []
    for date in all_dates:
        ht = ht_data.get(date, 0)
        nt = nt_data.get(date, 0)
        records.append({
            "date": date,
            "ht_kwh": round(ht, 2),
            "nt_kwh": round(nt, 2),
            "total_kwh": round(ht + nt, 2),
        })
    logger.info("parse_month_daily %s: %d records parsed", month_prefix, len(records))

    logger.info("Parsed %d days for %s (HT=%.1f, NT=%.1f kWh)", len(records), month_prefix,
                sum(r["ht_kwh"] for r in records), sum(r["nt_kwh"] for r in records))
    return records



# ---------------------------------------------------------------------------
# Backfill logic
# ---------------------------------------------------------------------------

def generate_month_range(start_date: str) -> list[tuple[int, int]]:
    """Generate (year, month) tuples from start_date to current month."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    now = datetime.now()
    months: list[tuple[int, int]] = []
    y, m = start.year, start.month
    while (y, m) <= (now.year, now.month):
        months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def run_backfill(session: requests.Session, installation_id: str, config: Config, cache: HistoryCache) -> int:
    """Fetch all missing months. Returns number of API calls made."""
    months = generate_month_range(config.backfill_from)
    api_calls = 0
    skipped = 0
    fetched = 0

    logger.info("Backfill: %d months from %s to now", len(months), config.backfill_from)

    for year, month in months:
        if cache.has_complete_month(year, month):
            skipped += 1
            continue

        raw = fetch_month_daily(session, installation_id, year, month, config.api_delay_seconds)
        api_calls += 1
        if raw is None:
            logger.warning("Failed to fetch %04d-%02d — skipping", year, month)
            continue

        records = parse_month_daily(raw, year, month)
        if records:
            cache.set_month(year, month, records)
            fetched += 1

        # Save after each month in case we get interrupted
        cache.save()

    logger.info("Backfill complete: %d fetched, %d cached/skipped, %d API calls", fetched, skipped, api_calls)
    return api_calls


# ---------------------------------------------------------------------------
# MQTT publishing
# ---------------------------------------------------------------------------

DEVICE_INFO = {
    "identifiers": ["ekz_energy_monitor"],
    "name": "EKZ Energy Monitor",
    "manufacturer": "EKZ",
    "model": "myEKZ Smart Meter",
    "sw_version": "4.0.0",
}


def publish_ha_discovery(client: mqtt.Client, config: Config) -> None:
    # Scalar sensors: (key, name, unit, dev_class, state_class, icon, sub_topic)
    sensors = [
        ("total_kwh", "Daily Total Consumption", "kWh", "energy", "total", "mdi:lightning-bolt", "daily"),
        ("peak_kwh", "Daily Peak Consumption", "kWh", "energy", "total", "mdi:sun-clock", "daily"),
        ("offpeak_kwh", "Daily Off-Peak Consumption", "kWh", "energy", "total", "mdi:moon-waning-crescent", "daily"),
        ("date", "Data Date", None, None, None, "mdi:calendar", "daily"),
        ("power", "Latest 15min Power", "W", "power", "measurement", "mdi:flash", "interval"),
        ("interval_kwh", "Latest 15min Energy", "kWh", "energy", "measurement", "mdi:meter-electric", "interval"),
        ("interval_time", "Latest Interval Time", None, "timestamp", None, "mdi:clock-outline", "interval"),
        # Meter sensors for HA Energy Dashboard (topic: meter)
        ("meter_ht", "Meter HT", "kWh", "energy", "total_increasing", "mdi:meter-electric", "meter"),
        ("meter_nt", "Meter NT", "kWh", "energy", "total_increasing", "mdi:meter-electric-outline", "meter"),
        # History-derived scalar sensors (topic: totals)
        ("ytd_total", "Year-to-Date Total", "kWh", "energy", None, "mdi:chart-timeline-variant", "totals"),
        ("ytd_ht", "Year-to-Date HT", "kWh", "energy", None, "mdi:sun-clock", "totals"),
        ("ytd_nt", "Year-to-Date NT", "kWh", "energy", None, "mdi:moon-waning-crescent", "totals"),
        ("mtd_total", "Month-to-Date Total", "kWh", "energy", None, "mdi:calendar-month", "totals"),
        ("mtd_ht", "Month-to-Date HT", "kWh", "energy", None, "mdi:sun-clock", "totals"),
        ("mtd_nt", "Month-to-Date NT", "kWh", "energy", None, "mdi:moon-waning-crescent", "totals"),
    ]

    for key, name, unit, dev_class, state_class, icon, sub_topic in sensors:
        topic = f"{config.ha_discovery_prefix}/sensor/ekz_energy/{key}/config"
        payload: dict[str, Any] = {
            "name": name,
            "unique_id": f"ekz_energy_{key}",
            "state_topic": f"{config.mqtt_topic_prefix}/{sub_topic}",
            "value_template": f"{{{{ value_json.{key} }}}}",
            "device": DEVICE_INFO,
            "icon": icon,
        }
        if unit:
            payload["unit_of_measurement"] = unit
        if dev_class:
            payload["device_class"] = dev_class
        if state_class:
            payload["state_class"] = state_class
        client.publish(topic, json.dumps(payload), retain=True)

    # Dedicated array entities (each has own json_attributes_topic)
    array_entities = [
        ("power_profile", "Power Profile", "profile", "mdi:chart-line"),
        ("daily_history", "Daily History", "daily_history", "mdi:chart-bar"),
        ("monthly_summary", "Monthly Summary", "monthly_summary", "mdi:chart-timeline-variant"),
    ]
    for key, name, sub_topic, icon in array_entities:
        topic = f"{config.ha_discovery_prefix}/sensor/ekz_energy/{key}/config"
        payload = {
            "name": name,
            "unique_id": f"ekz_energy_{key}",
            "state_topic": f"{config.mqtt_topic_prefix}/{sub_topic}",
            "value_template": "{{ value_json.state }}",
            "json_attributes_topic": f"{config.mqtt_topic_prefix}/{sub_topic}",
            "device": DEVICE_INFO,
            "icon": icon,
        }
        client.publish(topic, json.dumps(payload), retain=True)


def publish_daily_summary(client: mqtt.Client, summary: DailySummary, config: Config) -> None:
    datetimes = [iv.timestamp.strftime("%Y-%m-%dT%H:%M:%S") for iv in summary.intervals]
    watts = [round(iv.kwh * 4 * 1000, 1) for iv in summary.intervals]
    kwh_vals = [round(iv.kwh, 4) for iv in summary.intervals]
    ht_nt = ["HT" if iv.is_peak else "NT" for iv in summary.intervals]
    cum = []
    running = 0.0
    for iv in summary.intervals:
        running += iv.kwh
        cum.append(round(running, 4))

    # Scalars only on daily topic (no more profile arrays here)
    daily_state = {
        "total_kwh": summary.total_kwh, "peak_kwh": summary.peak_kwh, "offpeak_kwh": summary.offpeak_kwh,
        "date": summary.date,
    }
    client.publish(f"{config.mqtt_topic_prefix}/daily", json.dumps(daily_state), retain=True)

    # Profile arrays on dedicated topic
    profile_state = {
        "state": len(summary.intervals),
        "profile_datetimes": datetimes, "profile_watts": watts, "profile_kwh": kwh_vals,
        "profile_ht_nt": ht_nt, "profile_cumulative": cum,
    }
    client.publish(f"{config.mqtt_topic_prefix}/profile", json.dumps(profile_state), retain=True)
    logger.info("Published daily: %.2f kWh (%d intervals)", summary.total_kwh, len(summary.intervals))

    if summary.intervals:
        last = summary.intervals[-1]
        client.publish(f"{config.mqtt_topic_prefix}/interval", json.dumps({
            "power": round(last.kwh * 4 * 1000, 1), "interval_kwh": round(last.kwh, 4),
            "interval_time": last.timestamp.replace(tzinfo=LOCAL_TZ).isoformat(),
        }), retain=True)


def publish_history(client: mqtt.Client, cache: HistoryCache, config: Config) -> None:
    """Publish history-derived sensors split into focused topics."""
    now = datetime.now()
    ytd = cache.compute_yearly_totals(now.year)
    mtd = cache.compute_monthly_totals(now.year, now.month)

    # 1. Scalar totals only
    totals_payload = {
        "ytd_total": ytd["total_kwh"], "ytd_ht": ytd["ht_kwh"], "ytd_nt": ytd["nt_kwh"],
        "mtd_total": mtd["total_kwh"], "mtd_ht": mtd["ht_kwh"], "mtd_nt": mtd["nt_kwh"],
    }
    client.publish(f"{config.mqtt_topic_prefix}/totals", json.dumps(totals_payload), retain=True)

    # 2. Daily history arrays (last 90 days)
    all_days = cache.get_all_daily_records()
    recent_days = all_days[-90:] if len(all_days) > 90 else all_days
    daily_history_payload = {
        "state": len(recent_days),
        "history_dates": [d["date"] for d in recent_days],
        "history_ht": [d["ht_kwh"] for d in recent_days],
        "history_nt": [d["nt_kwh"] for d in recent_days],
        "history_total": [d["total_kwh"] for d in recent_days],
    }
    client.publish(f"{config.mqtt_topic_prefix}/daily_history", json.dumps(daily_history_payload), retain=True)

    # 3. Monthly summary arrays
    monthly_labels: list[str] = []
    monthly_ht: list[float] = []
    monthly_nt: list[float] = []
    monthly_total: list[float] = []
    for key in sorted(cache.data.keys()):
        m = cache.data[key]
        monthly_labels.append(key)
        monthly_ht.append(m.get("ht_kwh", 0))
        monthly_nt.append(m.get("nt_kwh", 0))
        monthly_total.append(m.get("total_kwh", 0))

    # Pre-compute Year-over-Year arrays (12 elements each, Jan-Dec)
    curr_year = str(now.year)
    prev_year = str(now.year - 1)

    def _year_array(labels: list[str], values: list[float], year: str) -> list[float]:
        result = [0.0] * 12
        for label, val in zip(labels, values):
            if label.startswith(year):
                month_idx = int(label.split("-")[1]) - 1
                result[month_idx] = val
        return result

    monthly_payload = {
        "state": len(monthly_labels),
        "monthly_labels": monthly_labels, "monthly_ht": monthly_ht, "monthly_nt": monthly_nt,
        "monthly_total": monthly_total,
        "total_prev_year": _year_array(monthly_labels, monthly_total, prev_year),
        "total_curr_year": _year_array(monthly_labels, monthly_total, curr_year),
    }
    client.publish(f"{config.mqtt_topic_prefix}/monthly_summary", json.dumps(monthly_payload), retain=True)

    # 4. Meter sensors — monotonically increasing totals for HA Energy Dashboard
    all_ht = sum(m.get("ht_kwh", 0) for m in cache.data.values())
    all_nt = sum(m.get("nt_kwh", 0) for m in cache.data.values())
    meter_payload = {"meter_ht": round(all_ht, 2), "meter_nt": round(all_nt, 2)}
    client.publish(f"{config.mqtt_topic_prefix}/meter", json.dumps(meter_payload), retain=True)

    logger.info("Published history: YTD=%.1f kWh, MTD=%.1f kWh, %d days, %d months, meter HT=%.1f NT=%.1f",
                ytd["total_kwh"], mtd["total_kwh"], len(all_days), len(monthly_labels), all_ht, all_nt)


# ---------------------------------------------------------------------------
# HA Energy Dashboard — hourly statistics import via WebSocket
# ---------------------------------------------------------------------------

def compute_hourly_deltas(summary: DailySummary) -> list[tuple[str, float, float]]:
    """Group 15-min intervals into hourly (ht_kwh, nt_kwh) deltas.

    Returns sorted list of (iso_timestamp, ht_delta, nt_delta) tuples.
    """
    if not summary.intervals:
        return []

    hourly_ht: dict[datetime, float] = {}
    hourly_nt: dict[datetime, float] = {}
    for iv in summary.intervals:
        hour_key = iv.timestamp.replace(minute=0, second=0, microsecond=0)
        if iv.is_peak:
            hourly_ht[hour_key] = hourly_ht.get(hour_key, 0) + iv.kwh
        else:
            hourly_nt[hour_key] = hourly_nt.get(hour_key, 0) + iv.kwh

    all_hours = sorted(set(hourly_ht) | set(hourly_nt))
    return [
        (hour.replace(tzinfo=LOCAL_TZ).isoformat(),
         hourly_ht.get(hour, 0),
         hourly_nt.get(hour, 0))
        for hour in all_hours
    ]


def _ws_recv_result(ws: Any, msg_id: int) -> dict[str, Any]:
    """Receive a WS result, skipping any interleaved event messages."""
    for _ in range(5):
        msg = json.loads(ws.recv())
        if msg.get("id") == msg_id:
            return msg
    return msg  # return whatever we got


def import_ha_statistics(config: Config, hourly_deltas: list[tuple[str, float, float]]) -> None:
    """Import hourly meter statistics into HA via WebSocket API.

    Uses *external statistics* (source ``ekz_energy``, colon-delimited IDs)
    so the recorder cannot interfere.  The user must add ``ekz_energy:meter_ht``
    and ``ekz_energy:meter_nt`` as grid-consumption entities in the HA Energy
    Dashboard (Settings → Dashboards → Energy).
    """
    if not hourly_deltas:
        return

    try:
        import websocket  # noqa: import only when needed
    except ImportError:
        logger.warning("websocket-client not installed — skipping HA statistics import")
        return

    ws_url = config.ha_url.replace("https://", "wss://").replace("http://", "ws://") + "/api/websocket"
    logger.info("Importing hourly statistics to HA (%d hours)...", len(hourly_deltas))

    try:
        ws = websocket.create_connection(ws_url, timeout=30)

        # 1. Auth
        msg = json.loads(ws.recv())
        if msg.get("type") != "auth_required":
            logger.warning("HA WS: unexpected initial message: %s", msg.get("type"))
            ws.close()
            return
        ws.send(json.dumps({"type": "auth", "access_token": config.ha_token}))
        msg = json.loads(ws.recv())
        if msg.get("type") != "auth_ok":
            logger.error("HA WS: auth failed: %s", msg.get("message", "unknown"))
            ws.close()
            return

        msg_id = 1
        first_hour = hourly_deltas[0][0]

        # 2. External statistics entities — owned by us, no recorder conflicts
        ext_sensors = [
            ("ekz_energy:meter_ht", "EKZ Meter HT (hourly)"),
            ("ekz_energy:meter_nt", "EKZ Meter NT (hourly)"),
        ]

        # Query existing external stats for reference (sum continuity)
        ref: dict[str, float] = {}  # statistic_id -> last sum
        for statistic_id, name in ext_sensors:
            ws.send(json.dumps({
                "id": msg_id,
                "type": "recorder/statistics_during_period",
                "start_time": "1970-01-01T00:00:00+00:00",
                "end_time": first_hour,
                "statistic_ids": [statistic_id],
                "period": "hour",
                "types": ["sum"],
            }))
            result = _ws_recv_result(ws, msg_id)
            msg_id += 1
            entries = result.get("result", {}).get(statistic_id, [])
            if entries:
                ref[statistic_id] = entries[-1].get("sum", 0)
                logger.info("HA WS: %s ref sum=%.2f", name, ref[statistic_id])
            else:
                ref[statistic_id] = 0.0

        # 3. Build and import stats for each sensor
        for statistic_id, name in ext_sensors:
            is_ht = "ht" in statistic_id
            running_sum = ref[statistic_id]
            stats: list[dict[str, Any]] = []

            for ts, ht_delta, nt_delta in hourly_deltas:
                delta = ht_delta if is_ht else nt_delta
                running_sum += delta
                stats.append({
                    "start": ts,
                    "state": round(running_sum, 4),
                    "sum": round(running_sum, 4),
                })

            ws.send(json.dumps({
                "id": msg_id,
                "type": "recorder/import_statistics",
                "metadata": {
                    "has_mean": False, "has_sum": True,
                    "statistic_id": statistic_id,
                    "source": "ekz_energy",
                    "name": name,
                    "unit_of_measurement": "kWh",
                },
                "stats": stats,
            }))
            result = _ws_recv_result(ws, msg_id)
            msg_id += 1

            if result.get("success") is True:
                logger.info("HA WS: %s — imported %d hours (sum %.2f→%.2f)",
                            name, len(stats), ref[statistic_id], running_sum)
            else:
                logger.warning("HA WS: %s import error: %s", name, result.get("error", result))

        # 4. Brief wait for async DB commit, then verify
        time.sleep(3)
        first_ts = hourly_deltas[0][0]
        last_ts = hourly_deltas[-1][0]
        for statistic_id, name in ext_sensors:
            ws.send(json.dumps({
                "id": msg_id,
                "type": "recorder/statistics_during_period",
                "start_time": first_ts,
                "end_time": last_ts,
                "statistic_ids": [statistic_id],
                "period": "hour",
                "types": ["sum"],
            }))
            result = _ws_recv_result(ws, msg_id)
            msg_id += 1
            entries = result.get("result", {}).get(statistic_id, [])
            if entries:
                logger.info("HA WS verify %s: %d entries, sum %.4f→%.4f",
                            name, len(entries),
                            entries[0].get("sum", 0), entries[-1].get("sum", 0))
            else:
                logger.warning("HA WS verify %s: NO entries found after import!", name)

        ws.close()
        logger.info("HA statistics import complete")

    except Exception as e:
        logger.warning("HA statistics import failed (non-fatal): %s", e)


def connect_mqtt(config: Config) -> mqtt.Client:
    client = mqtt.Client(client_id="ekz_energy_collector", protocol=mqtt.MQTTv311)
    if config.mqtt_username:
        client.username_pw_set(config.mqtt_username, config.mqtt_password)
    logger.info("Connecting to MQTT %s:%d...", config.mqtt_host, config.mqtt_port)
    client.connect(config.mqtt_host, config.mqtt_port, keepalive=60)
    client.loop_start()
    time.sleep(1)
    return client


# ---------------------------------------------------------------------------
# Main flows
# ---------------------------------------------------------------------------

def create_session(config: Config) -> tuple[requests.Session, str] | None:
    """Login and resolve installation ID. Returns (session, installation_id) or None."""
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 EKZ-HA-Collector/3.0"})

    if not ekz_login(session, config.ekz_username, config.ekz_password):
        return None

    installation_id = config.ekz_installation_id
    if not installation_id:
        result = discover_installation_id(session)
        if result is None:
            return None
        installation_id = result

    return session, installation_id


def run_daily_fetch(session: requests.Session, installation_id: str, config: Config,
                    cache: HistoryCache) -> DailySummary | None:
    """Fetch daily 15-min data + refresh current month's daily data."""
    # 1. Fetch 15-min detail for target day
    target_date = datetime.now() - timedelta(days=config.fetch_days_back)
    raw = fetch_15min(session, installation_id, target_date, config.api_delay_seconds)
    if raw is None:
        return None

    summary = parse_15min(raw, target_date, config)
    logger.info("%s | %.2f kWh (HT:%.2f NT:%.2f)",
                summary.date, summary.total_kwh, summary.peak_kwh, summary.offpeak_kwh)

    # 2. Refresh current month's daily data (1 extra API call)
    now = datetime.now()
    raw_month = fetch_month_daily(session, installation_id, now.year, now.month, config.api_delay_seconds)
    if raw_month:
        records = parse_month_daily(raw_month, now.year, now.month)
        if records:
            cache.set_month(now.year, now.month, records)
            cache.save()

    return summary


def run_once(config: Config, cache: HistoryCache) -> bool:
    result = create_session(config)
    if result is None:
        return False
    session, installation_id = result

    summary = run_daily_fetch(session, installation_id, config, cache)
    if summary is None:
        return False

    mqtt_client = connect_mqtt(config)
    try:
        publish_ha_discovery(mqtt_client, config)
        time.sleep(0.5)
        publish_history(mqtt_client, cache, config)

        if summary.total_kwh == 0 and not summary.intervals:
            logger.warning("No 15-min data for %s — daily summary skipped, history still published", summary.date)
        else:
            publish_daily_summary(mqtt_client, summary, config)
        time.sleep(1)
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()

    # Import hourly statistics into HA Energy Dashboard (if configured)
    if config.ha_url and config.ha_token and summary.intervals:
        hourly_deltas = compute_hourly_deltas(summary)
        import_ha_statistics(config, hourly_deltas)

    return True


def run_backfill_mode(config: Config, cache: HistoryCache) -> bool:
    result = create_session(config)
    if result is None:
        return False
    session, installation_id = result

    api_calls = run_backfill(session, installation_id, config, cache)
    logger.info("Backfill used %d API calls", api_calls)

    mqtt_client = connect_mqtt(config)
    try:
        publish_ha_discovery(mqtt_client, config)
        time.sleep(0.5)
        publish_history(mqtt_client, cache, config)
        time.sleep(1)
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
    return True


def main() -> None:
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        config = Config.from_env()
    except ValueError as e:
        logger.error("Config error: %s", e)
        sys.exit(1)

    cache = HistoryCache(config.data_dir)
    logger.info("EKZ Energy Collector v5.0 (mode=%s)", config.mode)

    if config.mode == "backfill":
        success = run_backfill_mode(config, cache)
        sys.exit(0 if success else 1)

    if config.mode == "oneshot":
        success = run_once(config, cache)
        sys.exit(0 if success else 1)

    # --- Daemon mode ---
    # Run backfill on first start if cache is empty
    if not cache.data:
        logger.info("Cache empty — running initial backfill...")
        result = create_session(config)
        if result:
            session, installation_id = result
            run_backfill(session, installation_id, config, cache)

    logger.info("Daemon mode — fetching every %.1fh", config.schedule_hours)

    shutdown = False

    def handle_signal(signum: int, _frame: Any) -> None:
        nonlocal shutdown
        logger.info("Signal %d — shutting down", signum)
        shutdown = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    while not shutdown:
        try:
            run_once(config, cache)
        except Exception:
            logger.exception("Error during fetch cycle")

        sleep_sec = config.schedule_hours * 3600
        next_run = datetime.now() + timedelta(seconds=sleep_sec)
        logger.info("Next fetch at ~%s", next_run.strftime("%H:%M"))

        slept = 0.0
        while slept < sleep_sec and not shutdown:
            time.sleep(min(30, sleep_sec - slept))
            slept += 30

    logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
