---
title: Murmur
emoji: 🐦
colorFrom: indigo
colorTo: pink
sdk: docker
app_port: 7860
pinned: false
---

# Murmur — a self-coordinating agent swarm on Aiven

**AI agents that don't just _use_ a database — they _operate_ one.** No backend, no human:
a swarm that coordinates over Apache Kafka, remembers in PostgreSQL + pgvector, and
**provisions + self-tunes its own Aiven infrastructure — all through the Aiven MCP.**

This Space runs the whole thing in one container: the **agent swarm** (the conductor) plus a
live read-only **dashboard** (the public page you're looking at). Every agent action is an MCP
tool call — there are no direct DB/Kafka drivers.

## What it does
- **Stagger + diversify** — each agent reads the shared Kafka stream and uses a **pgvector**
  similarity search to pick a measurably different hook at a staggered time. No controller.
- **Detect + amplify** — the fleet clusters the round's hooks in pgvector, finds the resonating
  theme, and the owning agent allocates ad budget to double down (Kafka `signals`).
- **Provision itself** — a new segment agent creates its **own Kafka topic + Postgres state** via the MCP.
- **Self-optimize** — a watcher agent reads its own query stats, sees the diversify search going
  full-scan at scale, and provisions a **pgvector HNSW index** itself (~23 ms → ~4 ms).
- **Decide autonomously** — an LLM **conductor** chooses the swarm's next move each cycle from live state.

## Run it locally
```bash
export ANTHROPIC_API_KEY=…   # claude-sonnet for the agents' decisions
export AIVEN_TOKEN=…         # Aiven personal token, for the MCP
uv run --with mcp --with anthropic --with fastembed python murmur.py --serve   # the autonomous conductor
```
Dashboard: `cd dashboard && uv run --with flask --with "psycopg[binary]" --with python-dotenv python app.py`

## Deploy (this Space)
One container runs both. Set three **Secrets** in the Space settings:
`ANTHROPIC_API_KEY`, `AIVEN_TOKEN`, and `DATABASE_URL` (the pg-conductor connection string,
`postgres://…?sslmode=require`). The dashboard serves on port 7860; the conductor runs alongside it.

## Stack
Aiven MCP (PostgreSQL + pgvector, Apache Kafka) · claude-sonnet for every decision ·
fastembed (local 384-dim embeddings) · `murmur.py` (the swarm) + a Flask dashboard.
