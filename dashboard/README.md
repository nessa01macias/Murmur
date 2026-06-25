# Murmur dashboard

A thin, **read-only** viewer that makes the Murmur swarm visible: a wall of account
tiles that light up as posts fire, a live feed that maps 1:1 to DB rows, and an
operator strip that shows budget shifting toward a resonating theme.

It reads **pg-conductor directly via psycopg** (`data.py`) — **not** through the Aiven
MCP, and it **never writes**. The MCP is agent-facing; this is a human viewer, so a
plain read-only Postgres connection is the correct seam.

## Run it

```bash
cd dashboard
pip install -r requirements.txt

# 1) Preview the layout right now with zero config (full mock, no DB):
MURMUR_MOCK=1 python app.py

# 2) Go live — paste the pg-conductor string into the env (Aiven needs TLS):
export DATABASE_URL='postgres://USER:PASS@HOST:PORT/defaultdb?sslmode=require'
python app.py
```

Open http://127.0.0.1:5050 . The page polls `/api/state` every 2s and re-renders.
(Or copy `.env.example` to `.env` and fill in `DATABASE_URL` — it's auto-loaded.)

## What's real vs mocked

Every section shows a **live / derived / mock** pill in the header so it's never
ambiguous on screen.

| Section | Live source | Falls back to |
|---|---|---|
| Account tiles | `accounts` table (real today: 2 rows) — auto-grows | the 2 seed accounts |
| Live feed | `posts` table (agent build adds it), else thin `events` rows | mock posts wired through **real** account ids |
| Operator/amplify | a `budget`/`themes` table, else derived from `posts.performance` | mock |

The mock posts deliberately stagger times, diversify hooks, and ramp one theme
("unreliable narrator") so all three demo beats are visible before real data flows.

## The one-line switch to live

`data.py` auto-detects the schema each poll. The moment a `posts` table exists with
rows, the feed and operator strip switch to live with **no code change**. It tolerates
column aliases (e.g. `hook`/`subject`, `ts`/`created_at`, `score`/`performance`) — see
the `*_CANDS` lists and the SCHEMA CONTRACT comment at the top of `data.py`.

## Security

- Connection comes from `$DATABASE_URL` only — never hardcoded, never logged.
- Connects with `default_transaction_read_only=on`; only SELECTs are issued.
- Binds to `127.0.0.1`. `.env` is gitignored.
