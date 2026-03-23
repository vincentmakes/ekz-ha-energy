# EKZ Energy Collector for Home Assistant

Fetches electricity consumption from **myEKZ** and publishes to **Home Assistant** via MQTT.
Includes historical backfill, 15-minute interval charts, and YTD/MTD tracking.

## Custom Dashboard
<img width="1175" height="642" alt="Screenshot 2026-03-23 at 16 11 13" src="https://github.com/user-attachments/assets/50abee06-a794-408d-a699-7e45be41d3f4" />
<img width="1123" height="387" alt="Screenshot 2026-03-23 at 16 11 22" src="https://github.com/user-attachments/assets/6e840037-8eec-4e73-874c-9d9922023a78" />
<img width="1101" height="389" alt="Screenshot 2026-03-23 at 16 11 31" src="https://github.com/user-attachments/assets/074aa67b-bd96-4532-8dd7-6232b14f2f32" />
<img width="1071" height="389" alt="Screenshot 2026-03-23 at 16 11 45" src="https://github.com/user-attachments/assets/22ff0330-d0bf-4378-be51-12b740e39426" />

## Energy Dashboard (out-of-the-box HA)  
<img width="1161" height="682" alt="Screenshot 2026-03-23 at 17 14 15" src="https://github.com/user-attachments/assets/231f72ca-6585-4627-b501-2a42310c6d19" />


Note there's a 4-8h delay when fetching the data as EKZ doesn't provide live data.  

## Features

- **Auto-discovers** your installation ID from myEKZ
- **Daily 15-minute intervals** with HT/NT split
- **Historical backfill** — fetches months of daily data from the EKZ API
- **Cached history** — completed months are never re-fetched
- **Rate limiting** — configurable delay between API calls (default 2s)
- **Daemon mode** with auto-backfill on first start
- **YTD / MTD totals** computed from actual API data (not just forward-looking)
- **Monthly + daily history charts** via apexcharts-card

## Quick Start

```bash
# 1. Copy files to Unraid
mkdir -p /mnt/user/appdata/ekz-ha-energy
# extract zip or copy files here

# 2. Configure
cp .env.example .env
nano .env   # Set EKZ_USERNAME, EKZ_PASSWORD, MQTT_HOST

# 3. Build and run
docker compose build
docker compose up -d

# 4. Watch the initial backfill
docker logs -f ekz-energy-collector
```

On first start with an empty cache, the collector automatically backfills
from `BACKFILL_FROM` (default: 2025-01-01). This takes ~30 seconds per month
(due to the 2s rate limit between API calls).

## Modes

| Mode | Usage | Description |
|------|-------|-------------|
| `daemon` | Default | Runs continuously, fetches every `SCHEDULE_HOURS`. Auto-backfills on first start. |
| `oneshot` | `MODE=oneshot docker compose run --rm ekz-collector` | Fetch once and exit. |
| `backfill` | `MODE=backfill docker compose run --rm ekz-collector` | Fetch all history and exit. Use to force a full re-fetch. |

## Rate Limiting

The collector respects `API_DELAY_SECONDS` (default: 2) between every API call.
A typical backfill of 15 months = ~15 API calls = ~30 seconds total.
In daemon mode, each cycle makes 2 API calls (15-min detail + current month daily).

## File Structure

| File | Description |
|------|-------------|
| `app/ekz_collector.py` | Main collector (login, fetch, parse, cache, MQTT) |
| `docker-compose.yml` | Unraid deployment with persistent `/data` volume |
| `.env.example` | Configuration template |
| `ha-dashboard.yaml` | HA dashboard (English) |
| `ha-dashboard-de.yaml` | HA dashboard (Deutsch) |
| `data/history.json` | Cached monthly data (auto-created, persisted via volume) |
| `claude.md` | Full project documentation |

## Home Assistant Setup

### 1. Install apexcharts-card
HACS → Frontend → Search "apexcharts-card" → Install → restart browser

### 2. Exclude array sensors from recorder
Add this to your `configuration.yaml` to prevent large JSON state changes
from bloating the HA database:

```yaml
recorder:
  exclude:
    entities:
      - sensor.ekz_energy_monitor_power_profile
      - sensor.ekz_energy_monitor_daily_history
      - sensor.ekz_energy_monitor_monthly_summary
```

Restart HA after adding this.

### 3. Create the dashboard
Settings → Dashboards → Add Dashboard → open → pencil icon → Raw config editor
→ paste `ha-dashboard.yaml` (EN) or `ha-dashboard-de.yaml` (DE) → Save

## Dashboard Contents

- **Daily KPIs** — consumption, HT, NT at a glance
- **Month/Year Totals** — cumulative kWh from API history
- **Power Profile** — 96-point 15-min area chart
- **Year-over-Year Monthly** — current vs previous year, side by side
- **HT/NT Donut** — year-to-date HT vs NT split
- **Avg Daily by Month** — normalized trend (kWh/day)
- **HT/NT Split + Cumulative** — today's 15-min details
- **Daily History (90 days)** — recent daily bars with HT/NT

## Native HA Energy Dashboard

The collector imports hourly external statistics into Home Assistant via WebSocket,
giving you **hourly resolution** in the built-in Energy Dashboard.

### Prerequisites

1. Set `HA_URL` and `HA_TOKEN` in your `.env` file (see `.env.example`)
   - **Use the IP address** of your HA instance (e.g. `http://192.168.1.100:8123`),
     not `homeassistant.local` — the Docker container must be able to reach HA
   - Create a Long-Lived Access Token: **Profile → Security → Long-Lived Access Tokens**

### Setup

1. Go to **Settings → Dashboards → Energy** → click pencil icon
2. **Remove** any old EKZ sensors (e.g. "EKZ Energy Monitor Meter HT/NT",
   "EKZ Monthly Energy offpeak", or anything with "EKZ" in the name)
3. Under **Electricity grid → Grid consumption**, click **Add consumption**
4. Add **two** entries:
   - `EKZ Meter HT (hourly)` (`ekz_energy:meter_ht`) — Peak (HT) consumption
   - `EKZ Meter NT (hourly)` (`ekz_energy:meter_nt`) — Off-Peak (NT) consumption
5. Save

This gives you **hourly/daily/weekly/monthly/yearly** views with HT/NT split.

> **Note:** EKZ data has a 24-48h delay. The custom apexcharts dashboard provides
> 15-minute detail for the most recent data.

## Configuration Reference

See `.env.example` for all options. Key settings:

| Variable | Default | Description |
|----------|---------|-------------|
| `BACKFILL_FROM` | `2025-01-01` | Start date for historical fetch |
| `API_DELAY_SECONDS` | `2` | Rate limit between API calls |
| `SCHEDULE_HOURS` | `4` | Fetch interval in daemon mode |
| `FETCH_DAYS_BACK` | `2` | Days back for 15-min detail |

## Forcing a Full Re-fetch

Delete the cache and run backfill:
```bash
rm data/history.json
MODE=backfill docker compose run --rm ekz-collector
```

## License

MIT — Based on [wesjdj/ekz-homeassistant-smart-meter](https://github.com/wesjdj/ekz-homeassistant-smart-meter)
