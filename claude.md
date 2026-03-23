# EKZ Energy Collector — Project Documentation

## Overview

Docker-based service that scrapes electricity consumption from the **myEKZ** portal and publishes to **Home Assistant** via MQTT. Features historical backfill, 15-minute interval data, and YTD/MTD tracking.

**Status:** Production on Unraid. v5.0.

## Architecture

```
myEKZ Portal (my.ekz.ch)
    ↓  HTTPS (Keycloak OAuth → portal-services API)
[ekz_collector.py]  ← Docker on Unraid (daemon mode)
    ↓  Reads/writes
[/data/history.json]    ← Monthly daily cache
    ↓  MQTT
[Mosquitto Broker]
    ↓
[Home Assistant]  ← HA OS VM on Unraid
    ├── MQTT sensors (auto-discovered)
    ├── apexcharts-card (profile + history charts)
    ├── HA Energy Dashboard (ekz_energy:meter_ht/nt via WS)
    └── Dashboard (EN + DE, single view)
```

## Files

| File | Purpose |
|------|---------|
| `app/ekz_collector.py` | Main script — login, fetch, parse, cache, MQTT |
| `app/requirements.txt` | requests, beautifulsoup4, paho-mqtt |
| `Dockerfile` | python:3.12-slim |
| `docker-compose.yml` | Daemon mode, `./data:/data` volume |
| `.env.example` | All configuration options |
| `ha-dashboard.yaml` | Dashboard (English) |
| `ha-dashboard-de.yaml` | Dashboard (Deutsch) |
| `data/history.json` | Auto-created cache of monthly daily data |

## myEKZ API

### Authentication
- **URL:** `https://my.ekz.ch/login` → Keycloak at `login.ekz.ch`
- **2FA:** Not supported — must be disabled
- **Session:** Cookie-based via `requests.Session`

### Endpoints

**Installation discovery:**
```
GET /api/portal-services/consumption-view/v1/installation-selection-data?installationVariant=CONSUMPTION
```
Key: `eanl[].anlage` where `sparte == "ELECTRICITY"`

**15-min intervals (7-day window):**
```
GET /api/portal-services/consumption-view/v1/consumption-data
    ?installationId={id}&from={date-6}&to={date+1}&type=PK_VERB_15MIN
```

**Daily totals (for backfill):**
```
GET /api/portal-services/consumption-view/v1/consumption-data
    ?installationId={id}&from={month-start}&to={month-end}&type=PK_VERB_TAG_METER
```

### Response Structure
```json
{
  "series": null,
  "seriesHt": { "tariffType": "HT", "values": [...] },
  "seriesNt": { "tariffType": "NT", "values": [...] }
}
```
Each value: `{ "value": 0.072, "date": "2026-03-21", "time": "00:00:00", "status": "VALID" }`

### Data Availability
- 24–48h delay from EKZ
- `FETCH_DAYS_BACK=2` for reliability

## Rate Limiting
- `API_DELAY_SECONDS=2` — sleep before each API call
- Backfill: ~1 call per month = ~15 calls for 15 months = ~30 seconds
- Daemon cycle: 2 calls (15-min + current month daily)
- Completed months cached in `history.json` and never re-fetched

## Cache (`data/history.json`)
```json
{
  "2025-01": {
    "year": 2025, "month": 1,
    "days": [{"date": "2025-01-01", "ht_kwh": 8.0, "nt_kwh": 4.0, "total_kwh": 12.0}, ...],
    "ht_kwh": 150.0, "nt_kwh": 100.0, "total_kwh": 250.0,
    "fetched_at": "2026-03-22T19:00:00"
  }
}
```

## MQTT Topics

| Topic | Content |
|-------|---------|
| `ekz_energy/daily` | Scalar daily values (total_kwh, peak_kwh, offpeak_kwh, date) |
| `ekz_energy/profile` | 15-min arrays (datetimes, watts, kwh, ht_nt, cumulative) |
| `ekz_energy/interval` | Latest 15-min reading (power, interval_kwh, interval_time) |
| `ekz_energy/meter` | HT + NT meter readings (total_increasing, for HA Energy Dashboard) |
| `ekz_energy/totals` | YTD/MTD scalar totals (ytd_total/ht/nt, mtd_total/ht/nt) |
| `ekz_energy/daily_history` | 90-day daily arrays (dates, ht, nt, total) |
| `ekz_energy/monthly_summary` | Monthly arrays (labels, ht, nt, total, YoY) |

## HA Sensors

### Scalar Sensors (MQTT Auto-Discovery)
| Entity ID | Source |
|-----------|--------|
| `sensor.ekz_energy_monitor_daily_total_consumption` | daily |
| `sensor.ekz_energy_monitor_daily_peak_consumption` | daily |
| `sensor.ekz_energy_monitor_daily_off_peak_consumption` | daily |
| `sensor.ekz_energy_monitor_data_date` | daily |
| `sensor.ekz_energy_monitor_latest_15min_power` | interval |
| `sensor.ekz_energy_monitor_latest_15min_energy` | interval |
| `sensor.ekz_energy_monitor_latest_interval_time` | interval |
| `sensor.ekz_energy_monitor_meter_ht` | meter |
| `sensor.ekz_energy_monitor_meter_nt` | meter |
| `sensor.ekz_energy_monitor_year_to_date_total` | totals |
| `sensor.ekz_energy_monitor_year_to_date_ht` | totals |
| `sensor.ekz_energy_monitor_year_to_date_nt` | totals |
| `sensor.ekz_energy_monitor_month_to_date_total` | totals |
| `sensor.ekz_energy_monitor_month_to_date_ht` | totals |
| `sensor.ekz_energy_monitor_month_to_date_nt` | totals |

### Array Sensors (for apexcharts-card)
| Entity ID | Source | Key Attributes |
|-----------|--------|----------------|
| `sensor.ekz_energy_monitor_power_profile` | profile | profile_datetimes, profile_watts, profile_kwh, profile_ht_nt, profile_cumulative |
| `sensor.ekz_energy_monitor_daily_history` | daily_history | history_dates, history_ht, history_nt, history_total |
| `sensor.ekz_energy_monitor_monthly_summary` | monthly_summary | monthly_labels, monthly_ht, monthly_nt, monthly_total, total_prev_year, total_curr_year |

### HA Energy Dashboard (External Statistics)
The collector imports hourly external statistics via HA WebSocket for the native Energy Dashboard:
- `ekz_energy:meter_ht` — HT (Peak) consumption, hourly cumulative sum
- `ekz_energy:meter_nt` — NT (Off-Peak) consumption, hourly cumulative sum

Requires `HA_URL` (IP address, e.g. `http://192.168.1.100:8123`) and `HA_TOKEN` (Long-Lived Access Token) in `.env`.
Provides **hourly resolution** in the Energy Dashboard. Data imported each fetch cycle (last 72 hours).

## Configuration

See `.env.example`. Key options:

| Variable | Default | Description |
|----------|---------|-------------|
| `BACKFILL_FROM` | `2025-01-01` | Start date for history fetch |
| `API_DELAY_SECONDS` | `2` | Rate limit between API calls |
| `SCHEDULE_HOURS` | `4` | Fetch interval in daemon mode |
| `FETCH_DAYS_BACK` | `2` | Days back for 15-min detail |
| `HA_URL` | — | HA IP address (e.g. `http://192.168.1.100:8123`) for hourly stats |
| `HA_TOKEN` | — | Long-Lived Access Token for HA WebSocket |

## Modes

| Mode | Behavior |
|------|----------|
| `daemon` | Continuous. Auto-backfills if cache empty. Fetches every SCHEDULE_HOURS. |
| `oneshot` | Single fetch + publish, then exit. |
| `backfill` | Fetch all months from BACKFILL_FROM, publish history, exit. |

## Dashboard

Single-view dashboard (EN + DE) with sections in this order:

1. **KPIs** — Today (total, HT, NT) + Cumulative (month, year, data date)
2. **Power Profile** — 2-day 15-min area chart
3. **Today Detail** — HT/NT split bar + cumulative line (side by side)
4. **Daily History** — 90-day stacked HT/NT bars
5. **Monthly YoY** — current vs previous year bar chart
6. **Details + Donut** — HT/NT breakdown table + YTD donut (side by side)
7. **Avg Daily** — kWh/day trend by month

## Changelog

### v5.1 (2026-03-23)
- **Hourly Energy Dashboard**: External statistics via HA WebSocket (`ekz_energy:meter_ht/nt`) with hourly resolution
- **HA_URL + HA_TOKEN config**: Required for hourly stats import; must use HA IP address (not hostname)

### v5.0 (2026-03-23)
- **Removed costs, billing, and tariffs**: No more cost calculations, billing periods, or tariff rate fetching
- **Added HT/NT meter sensors**: Two `total_increasing` sensors for HA Energy Dashboard
- **Single-view dashboard**: All charts in one scrollable view
- **Fixed sensor naming**: Entity IDs now match dashboard references
- **Removed legacy sensors**: cumulative_kwh, interval_tariff, utility_meters
- **Renamed profile_tariffs → profile_ht_nt**: Clearer naming
- **Fixed docker-compose**: Removed hardcoded LOG_LEVEL=DEBUG

### v4.0 (2026-03-23)
- Split MQTT entities by granularity

### v3.0–3.1 (2026-03-22)
- Historical backfill, persistent cache, rate limiting
- YTD/MTD from API data, monthly + daily history charts

### v2.0 (2026-03-22)
- Daemon mode, auto-discovery, 15-min profiles

### v1.0 (2026-03-22)
- Initial: login, fetch, MQTT publish
