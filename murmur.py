#!/usr/bin/env python3
"""
Murmur swarm — a self-coordinating account-agent fleet on Aiven.

A fleet of book-marketing account-agents runs a content network with no human and no
backend server. They coordinate peer-to-peer over Aiven Kafka and remember in Aiven
Postgres + pgvector — ALL data operations go through the Aiven MCP. This script is just
an MCP client, claude-sonnet for the agents' decisions, and fastembed for local embeddings.

Three coordinated behaviours (all require the shared Kafka stream — remove Aiven and they
collapse), with deliberately NO cross-account engagement (no likes/boosts/replies):

  STAGGER + DIVERSIFY  Each agent reads the `posts` stream, then picks a subject that is
                       measurably different (pgvector cosine vs peers' hooks) at a time
                       staggered from peers — so the fleet never collides or repeats.
  DETECT               After the round, the fleet clusters the round's hooks in pgvector
                       to find the best-performing theme and emits a `signals` message.
  AMPLIFY              The agent that owns the resonating theme allocates ad budget and
                       produces a doubling-down post (legitimate marketing ops).

Run it (and watch the coordination print out). Put your secrets in a .env next to this
script (auto-loaded) or export them:

    ANTHROPIC_API_KEY=sk-ant-...   # for claude-sonnet decisions
    AIVEN_TOKEN=...                # Aiven personal token, for the MCP
then:
    uv run --with mcp --with anthropic --with fastembed python murmur.py
    MURMUR_FLEET=10 uv run --with mcp --with anthropic --with fastembed python murmur.py  # scale up
    uv run --with mcp --with anthropic --with fastembed --with claude-agent-sdk python murmur.py --onboard "cozy-mystery"
    #   ^ onboarding is SDK-driven: a REAL Claude Agent SDK agent decides to call the Aiven MCP
    #     provisioning tools ITSELF (own Kafka lane + Postgres state + announce), then hands off to
    #     the deterministic diversify-join. Needs the Claude Code CLI: npm i -g @anthropic-ai/claude-code
    uv run --with mcp --with anthropic --with fastembed python murmur.py --tier2
    #   ^ the watcher agent observes its own DB load, then provisions a pgvector index to self-optimize
    # first run downloads a small (~130MB) local embedding model, one time

Transport: connects to the hosted Aiven MCP (https://mcp.aiven.live/mcp) using AIVEN_TOKEN
as a bearer; if that endpoint lacks the write tools (read-only), it falls back to spawning
the bundled local server (./mcp-aiven, built with `npm install && npm run build`) over stdio.

Autonomous mode: `python murmur.py --serve` runs the CONDUCTOR loop — a status endpoint on
$PORT (default 8080) plus, every $MURMUR_INTERVAL seconds (default 180), an LLM decision on the
swarm's next move (round / onboard / optimize / idle) from its own live state. No human, no
fixed schedule of actions. The Dockerfile builds exactly this; run it under launchd/cron or
(when access is granted) Aiven Apps with ANTHROPIC_API_KEY + AIVEN_TOKEN as secrets — all via the MCP.
"""

import asyncio
import json
import os
import random
import re
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamablehttp_client
from anthropic import Anthropic

# ----------------------------------------------------------------------------- config
PROJECT    = "joinpharos-6858"
KAFKA_SVC  = "kafka-conductor"
PG_SVC     = "pg-conductor"
POSTS      = "posts"            # coordination bus: what each agent is about to post
SIGNALS    = "signals"         # trend signals + budget-amplify decisions
FORMAT     = "json"            # Kafka REST embedded format; MUST match for produce + consume
MODEL      = "claude-sonnet-4-6"
MCP_URL    = "https://mcp.aiven.live/mcp"
LOCAL_SERVER = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "mcp-aiven", "dist", "index.js")

# Embeddings + diversity. Within one genre, baseline cosine is ~0.5-0.7 and rephrasings
# ~0.85+, so 0.80 rejects near-duplicates ("a different label") while allowing real angles.
EMBED_MODEL = "BAAI/bge-small-en-v1.5"   # fastembed, 384-dim, no API key
SIM_REJECT = 0.80
DIVERSIFY_TRIES = 3
THEME_SIM = 0.55          # round hooks within this cosine of the leader form the "theme"
STAGGER_SECONDS = 1.0     # pause between agents — gentle on MCP rate limits + watchable

# Tier 2 (self-optimization, capacity rehearsal). Validated live: HNSW (m=8, ef_construction=32)
# builds <30s up to ~20k 384-dim rows; 50k exceeds the 30s timeout. 20k: seq-NN ~19ms -> HNSW ~3ms,
# recall@10 100%. Runs on a throwaway per-run bench table — NEVER the live `hooks` (the write tool
# blocks DROP, so a timed-out build would be unrecoverable on prod; and a pre-existing index would
# ruin the seq baseline). 384-dim probe vector reused for the before/after + recall.
TIER2_ROWS = int(os.environ.get("MURMUR_TIER2_ROWS", "20000"))
TIER2_CHUNK = 20000       # rows per insert (each must finish < the 30s statement timeout)
PROBE = "(SELECT array_agg(0.5)::vector FROM generate_series(1,384))"

# The fleet roster (same genre, distinct voices, so they compete for one lane and must
# diversify). MURMUR_FLEET picks how many run (default 6); framed as scalable to dozens.
GENRE = "mystery"
ROSTER = [
    ("acct_01", "Nova — hypes clever twists and slow-burn reveals"),
    ("acct_02", "Echo — keeps the feed fresh and starts conversations"),
    ("acct_03", "Sable — devoted to noir and hardboiled detectives"),
    ("acct_04", "Wren — champions cozy mysteries, tea-and-cardigan vibes"),
    ("acct_05", "Cole — a forensic-detail nerd who loves the procedure"),
    ("acct_06", "Iris — obsessed with locked-room and impossible crimes"),
    ("acct_07", "Dash — chases fast thrillers and cliffhangers"),
    ("acct_08", "Vesper — drawn to psychological suspense and unreliable minds"),
    ("acct_09", "Quill — savours historical mysteries and period atmosphere"),
    ("acct_10", "Juno — runs whodunit guessing games with readers"),
]
FLEET_SIZE = max(1, min(len(ROSTER), int(os.environ.get("MURMUR_FLEET", "6"))))

AC: Anthropic = None  # Anthropic client, created in main() after env check

# ----------------------------------------------------------------------------- pretty print
DIM, BOLD, CYAN, MAGENTA, GREEN, YELLOW, BLUE, RESET = (
    "\033[2m", "\033[1m", "\033[36m", "\033[35m", "\033[32m", "\033[33m", "\033[34m", "\033[0m"
)
PALETTE = [CYAN, MAGENTA, GREEN, YELLOW, BLUE]

def rule():
    print(DIM + "─" * 72 + RESET)

def banner(color, label, line):
    print(f"\n{color}{BOLD}{label}{RESET} {color}{line}{RESET}")

def step(line):
    print(f"   {DIM}→{RESET} {line}")

def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ----------------------------------------------------------------------------- json helpers
def extract_json(text):
    """Pull a JSON value out of an MCP/LLM text payload, tolerant of wrappers, prose, or
    markdown fences. Robust to the MCP 'untrusted data' envelope even when the payload was
    TRUNCATED at the output cap (closing tag missing), and never scans O(n^2) on big blobs."""
    if text is None:
        raise ValueError("empty tool/LLM result")
    # unwrap the MCP untrusted-data envelope; the closing tag may be absent if truncated
    m = re.search(r"<untrusted-[^>]*>(.*?)(?:</untrusted-[^>]*>|\Z)", text, re.DOTALL)
    if m:
        text = m.group(1)
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\n?|\n?```$", "", text).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # decode the first JSON value in O(n) via raw_decode (no quadratic backward scan)
    starts = [i for i in (text.find("{"), text.find("[")) if i != -1]
    if not starts:
        raise ValueError(f"no JSON found in: {text[:200]!r}")
    try:
        obj, _ = json.JSONDecoder().raw_decode(text, min(starts))
        return obj
    except json.JSONDecodeError:
        raise ValueError(f"could not parse JSON from: {text[:200]!r}")

# ----------------------------------------------------------------------------- embeddings (local)
_EMBEDDER = None
def embed(text):
    """Local 384-dim embedding via fastembed (lazy-loaded, no API key)."""
    global _EMBEDDER
    if _EMBEDDER is None:
        from fastembed import TextEmbedding
        _EMBEDDER = TextEmbedding(model_name=EMBED_MODEL)
    return list(_EMBEDDER.embed([text]))[0]

def vec_literal(vec):
    """Format a vector as a pgvector literal, e.g. '[0.1,0.2,...]'."""
    return "[" + ",".join(f"{float(x):.6f}" for x in vec) + "]"

def sql_str(s):
    """Escape a string for single-quoted SQL (LLM-authored subjects reach SQL via hooks)."""
    return s.replace("'", "''")

# ----------------------------------------------------------------------------- MCP layer
async def call(session, tool, args, retries=4):
    """Call an Aiven MCP tool, returning its text payload. Retries transient errors
    (429 rate limits, Kafka-REST 'temporarily unavailable') with exponential backoff."""
    delay = 1.5
    for attempt in range(retries):
        res = await session.call_tool(tool, args)
        text = "".join(getattr(c, "text", "") for c in res.content)
        if not res.isError:
            return text
        low = text.lower()
        transient = any(s in low for s in (
            "429", "rate limit", "too many requests", "temporarily",
            "service unavailable", "503", "timed out", "timeout",
            # transient DNS / connection blips between the MCP server and Postgres/Kafka
            "eai_again", "getaddrinfo", "connection error", "econnreset", "etimedout"))
        if transient and attempt < retries - 1:
            await asyncio.sleep(delay)
            delay *= 2
            continue
        raise RuntimeError(f"MCP tool {tool} failed: {text[:400]}")

async def pg_read(session, sql):
    txt = await call(session, "aiven_pg_read", {
        "project": PROJECT, "service_name": PG_SVC,
        "query": sql, "reasoning": "murmur swarm"})
    return extract_json(txt).get("rows", [])

async def pg_write(session, sql):
    await call(session, "aiven_pg_write", {
        "project": PROJECT, "service_name": PG_SVC,
        "query": sql, "reasoning": "murmur swarm"})

async def produce(session, topic, records):
    txt = await call(session, "aiven_kafka_topic_message_produce", {
        "project": PROJECT, "service_name": KAFKA_SVC, "topic_name": topic,
        "format": FORMAT, "records": records})
    return extract_json(txt)

async def consume(session, topic=POSTS, offset=0):
    txt = await call(session, "aiven_kafka_topic_message_list", {
        "project": PROJECT, "service_name": KAFKA_SVC, "topic_name": topic,
        "partitions": {"0": {"offset": offset}}, "format": FORMAT,
        "timeout": 8000, "max_bytes": 60000})   # bound the payload under the MCP output cap
    parsed = extract_json(txt)
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict):
        for k in ("messages", "records", "data"):
            if isinstance(parsed.get(k), list):
                return parsed[k]
    return []

async def latest_offset(session, topic=POSTS):
    """High-water offset of the topic, regex-scanned straight from the metadata text (robust to
    the untrusted-data wrapper + any truncation), so agents read only the RECENT tail. Reading
    from offset 0 returns the whole backlog, which blows past the MCP output cap (100k chars)
    and comes back truncated — and gets slower every round as the topic grows."""
    txt = await call(session, "aiven_kafka_topic_get", {
        "project": PROJECT, "service_name": KAFKA_SVC, "topic_name": topic})
    offs = [int(x) for x in re.findall(r'"latest_offset"\s*:\s*(\d+)', txt)]
    return max(offs) if offs else 0

def msg_value(rec):
    """Normalize a consumed record's `value` to a dict."""
    v = rec.get("value") if isinstance(rec, dict) else None
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except json.JSONDecodeError:
            v = {"text": v}
    return v if isinstance(v, dict) else {}

async def store_hook(session, account_id, subject, vec, performance):
    """Persist a post's subject embedding + simulated performance into pgvector (via MCP)."""
    await pg_write(session,
        f"INSERT INTO hooks (account_id, subject, embedding, performance) VALUES "
        f"('{sql_str(account_id)}', '{sql_str(subject)}', '{vec_literal(vec)}', {int(performance)})")

async def nearest_peer_hook(session, me_id, vec, since):
    """Most-similar peer hook THIS round as (account_id, subject, cosine_sim), or None."""
    lit = vec_literal(vec)
    rows = await pg_read(session,
        f"SELECT account_id, subject, 1 - (embedding <=> '{lit}') AS sim FROM hooks "
        f"WHERE account_id <> '{sql_str(me_id)}' AND ts >= '{since}' "
        f"ORDER BY embedding <=> '{lit}' LIMIT 1")
    if not rows:
        return None
    return rows[0]["account_id"], rows[0]["subject"], float(rows[0]["sim"])

async def store_post(session, account_id, subject, body, performance, diversified_from, scheduled_for):
    """Durable, dashboard-readable record of a post (via MCP). Kafka/events/hooks unchanged."""
    div = f"'{sql_str(diversified_from)}'" if diversified_from else "NULL"
    perf = "NULL" if performance is None else f"{float(performance)}"
    await pg_write(session,
        f"INSERT INTO posts (account_id, subject, body, performance, diversified_from, scheduled_for) "
        f"VALUES ('{sql_str(account_id)}', '{sql_str(subject)}', '{sql_str(body)}', "
        f"{perf}, {div}, '{sql_str(scheduled_for)}')")

async def store_signal(session, kind, payload):
    """Durable, dashboard-readable copy of a Kafka `signals` event (trend/amplify/optimize),
    so the wall can show the real autonomous decisions — not a re-derived stand-in."""
    await pg_write(session,
        f"INSERT INTO signals (kind, payload) VALUES "
        f"('{sql_str(kind)}', '{sql_str(json.dumps(payload))}'::jsonb)")

async def set_activity(session, account_id, state, detail=""):
    """Live work-trail: record what an agent is doing RIGHT NOW (reading → thinking →
    rephrasing → posting → amplifying → provisioning) so the dashboard can show the swarm
    *working*, not just its finished rows. Append-only; the wall reads the latest per agent.
    Best-effort — a status write must NEVER break a round, so failures are swallowed."""
    try:
        await pg_write(session,
            f"INSERT INTO activity (account_id, state, detail) VALUES "
            f"('{sql_str(account_id)}', '{sql_str(state)}', '{sql_str(detail)}')")
    except Exception:  # noqa: BLE001 — cosmetic liveness only
        pass

# --- Tier 2 observe/optimize MCP wrappers ---
async def query_stats(session, order_by="total_time:desc", limit=5, search=None):
    args = {"project": PROJECT, "service_name": PG_SVC, "order_by": order_by, "limit": limit}
    if search:
        args["search"] = search
    txt = await call(session, "aiven_pg_service_query_statistics", args)
    return extract_json(txt).get("queries", [])

async def db_metrics(session, period="hour"):
    txt = await call(session, "aiven_service_metrics_fetch",
                     {"project": PROJECT, "service_name": PG_SVC, "period": period})
    return extract_json(txt)

async def account_id(session):
    p = extract_json(await call(session, "aiven_project_get", {"project": PROJECT}))
    return (p.get("project") or p).get("account_id")

async def optimize_query(session, acct, query):
    txt = await call(session, "aiven_pg_optimize_query",
                     {"account_id": acct, "query": query, "pg_version": "17",
                      "reasoning": "murmur tier2 advisory cross-check"})
    return extract_json(txt)

async def explain(session, sql, analyze=True):
    """Return (plan_text, execution_ms, scan_kind) for a query via aiven_pg_read."""
    rows = await pg_read(session, ("EXPLAIN (ANALYZE) " if analyze else "EXPLAIN ") + sql)
    plan = "\n".join(r.get("QUERY PLAN", "") for r in rows)
    m = re.search(r"Execution Time: ([\d.]+) ms", plan)
    ms = float(m.group(1)) if m else None
    scan = "Index Scan" if "Index Scan" in plan else ("Seq Scan" if "Seq Scan" in plan else "?")
    return plan, ms, scan

async def topk_ids(session, table, k=10):
    rows = await pg_read(session, f"SELECT id FROM {table} ORDER BY embedding <=> {PROBE} LIMIT {k}")
    return [r["id"] for r in rows]

# ----------------------------------------------------------------------------- LLM layer
def decide(system, user):
    """Ask claude-sonnet for a decision; returns the parsed JSON object (a dict)."""
    msg = AC.messages.create(
        model=MODEL, max_tokens=400, system=system,
        messages=[{"role": "user", "content": user}])
    raw = "".join(b.text for b in msg.content if getattr(b, "type", None) == "text")
    return extract_json(raw)

def persona_system(me):
    return (f"You are {me['persona']}. You are an autonomous account-agent in a book-marketing "
            f"fleet. Your genre is '{me['genre']}'. You think briefly, in character, then act. "
            f"Always answer with a single JSON object only.")

def conductor_decide(state):
    """The autonomous conductor: decide the swarm's NEXT MOVE from its live state (LLM, not a timer)."""
    msg = AC.messages.create(
        model=MODEL, max_tokens=220,
        system="You are the autonomous conductor of Murmur, a self-running book-marketing agent swarm "
               "on Aiven. You decide the swarm's next move from its live state — there is no human and "
               "no schedule. Answer with a single JSON object only.",
        messages=[{"role": "user", "content":
            f"Live state: {state['agents']} agents · {state['recent_posts']} posts in the last 30min · "
            f"{state['mins_since_post']} min since the last post · {state['hooks']} hooks in pgvector memory · "
            f"last self-optimize {state['mins_since_optimize']} min ago · resonating theme: \"{state['top_theme']}\".\n"
            "Decide the next move. Options: 'round' (agents post, diversify vs peers, the fleet detects a "
            "trend & amplifies it); 'onboard' (recruit a NEW audience-segment agent — give a 2-4 word "
            "segment); 'optimize' (self-tune the pgvector memory index — only worthwhile once memory has "
            "grown a lot); 'idle' (wait, if it just acted). Keep the network alive and varied; don't "
            "onboard every cycle. Reply ONLY JSON: "
            '{"action":"round|onboard|optimize|idle","segment":"<only if onboard>","reason":"<one sentence>"}'}])
    raw = "".join(b.text for b in msg.content if getattr(b, "type", None) == "text")
    return extract_json(raw)

# ----------------------------------------------------------------------------- setup (idempotent)
async def ensure_schema(session):
    """Create the tables, pgvector extension, and shared topics — idempotent, all via MCP."""
    await pg_write(session,
        "CREATE TABLE IF NOT EXISTS accounts (id text PRIMARY KEY, persona text NOT NULL, genre text NOT NULL)")
    await pg_write(session,
        "CREATE TABLE IF NOT EXISTS events (id bigserial PRIMARY KEY, account_id text NOT NULL, "
        "type text NOT NULL, topic text NOT NULL, ts timestamptz NOT NULL DEFAULT now())")
    await pg_write(session, "CREATE EXTENSION IF NOT EXISTS vector")
    await pg_write(session,
        "CREATE TABLE IF NOT EXISTS hooks (id bigserial PRIMARY KEY, account_id text NOT NULL, "
        "subject text NOT NULL, embedding vector(384) NOT NULL, performance int, "
        "ts timestamptz NOT NULL DEFAULT now())")
    await pg_write(session,
        "CREATE TABLE IF NOT EXISTS posts (id bigserial PRIMARY KEY, "
        "account_id text REFERENCES accounts(id), subject text, body text, performance numeric, "
        "diversified_from text, scheduled_for text, created_at timestamptz NOT NULL DEFAULT now())")
    await pg_write(session,
        "CREATE TABLE IF NOT EXISTS signals (id bigserial PRIMARY KEY, kind text NOT NULL, "
        "payload jsonb NOT NULL, ts timestamptz NOT NULL DEFAULT now())")
    await pg_write(session,
        "CREATE TABLE IF NOT EXISTS activity (id bigserial PRIMARY KEY, account_id text NOT NULL, "
        "state text NOT NULL, detail text, ts timestamptz NOT NULL DEFAULT now())")
    for topic in (POSTS, SIGNALS):
        try:  # topics are usually pre-created; tolerate "already exists"
            await call(session, "aiven_kafka_topic_create", {
                "project": PROJECT, "service_name": KAFKA_SVC,
                "topic_name": topic, "partitions": 1, "replication": 1})
        except Exception:
            pass

async def setup(session):
    banner(YELLOW, "[setup]", "ensuring tables, fleet, topics, pgvector (all via MCP)")
    await ensure_schema(session)
    for acct_id, persona in ROSTER[:FLEET_SIZE]:
        await pg_write(session,
            f"INSERT INTO accounts (id, persona, genre) VALUES "
            f"('{acct_id}', '{sql_str(persona)}', '{GENRE}') "
            f"ON CONFLICT (id) DO UPDATE SET persona = EXCLUDED.persona, genre = EXCLUDED.genre")
    step(f"fleet of {FLEET_SIZE} {GENRE} agents ready: "
         + ", ".join(a for a, _ in ROSTER[:FLEET_SIZE]))

async def load_account(session, acct_id):
    rows = await pg_read(session, f"SELECT id, persona, genre FROM accounts WHERE id = '{sql_str(acct_id)}'")
    if not rows:
        raise SystemExit(f"account {acct_id} not found — run setup first")
    return rows[0]

# ----------------------------------------------------------------------------- one agent's turn
async def agent_post(session, acct_id, color, since_iso, since_db):
    """One account-agent: read the stream, choose a measurably-different subject at a
    staggered time, and publish. `since_*` scope coordination to the current round."""
    me = await load_account(session, acct_id)
    banner(color, f"▶ {acct_id}", f"{me['persona'].split(' — ')[0]} · {me['genre']}")
    await set_activity(session, me["id"], "reading", "scanning the stream for what peers just planned")

    # read only the RECENT tail of the Kafka stream (the coordination bus) — reading from
    # offset 0 returns the whole backlog, which exceeds the MCP output cap and truncates
    try:
        hw = await latest_offset(session, POSTS)
    except Exception:
        hw = 0
    start_off = max(0, hw - 40)     # ~a round's worth of recent posts, well under the cap
    peers = []
    for _ in range(5):
        try:
            recs = await consume(session, POSTS, start_off)
        except Exception as e:
            step(f"{DIM}stream not ready ({str(e)[:40]}…), retrying{RESET}")
            await asyncio.sleep(1.5)
            continue
        latest = {}
        for rec in recs:
            v = msg_value(rec)
            if (v.get("type") == "post" and v.get("subject") and v.get("ts", "") >= since_iso
                    and v.get("genre") == me["genre"] and v.get("account_id") != me["id"]):
                latest[v["account_id"]] = v   # keep this round's latest per peer
        peers = list(latest.values())
        break
    taken = "; ".join(f'"{p["subject"]}" @ {p.get("scheduled_for")}' for p in peers) or "(nothing yet — you're first)"
    step(f"stream shows {len(peers)} peer post(s) this round: {DIM}{taken}{RESET}")

    base = (f"Your '{me['genre']}' fleet has already planned these posts this round:\n  {taken}\n\n"
            "Choose a post with a DIFFERENT subject/hook from all of those (not a rephrase) and a "
            "STAGGERED time clearly apart from the times listed. ")
    tail = ('Reply with ONLY JSON: {"subject": "<your distinct topic/hook, 2-5 words>", '
            '"text": "<your post, max ~180 chars>", "scheduled_for": "<a free time slot today>", '
            '"reason": "<one short sentence: why this is distinct>"}')

    # diversify-until-different: MEASURE each candidate against peers' hooks in pgvector
    subject = text = when = reason = ""
    vec, sim, near_acct, near_sub, avoid = None, 0.0, None, None, ""
    for attempt in range(1, DIVERSIFY_TRIES + 1):
        print(f"   {DIM}thinking with {MODEL} (try {attempt})…{RESET}")
        await set_activity(session, me["id"], "thinking", f"drafting a hook unlike the others (try {attempt})")
        d = await asyncio.to_thread(decide, persona_system(me), base + avoid + tail)
        subject, text = d.get("subject", "").strip(), d.get("text", "").strip()
        when, reason = d.get("scheduled_for", "").strip(), d.get("reason", "").strip()
        vec = await asyncio.to_thread(embed, subject)
        near = await nearest_peer_hook(session, me["id"], vec, since_db)
        if near:
            near_acct, near_sub, sim = near
        if sim <= SIM_REJECT:
            tag = (f"only {sim:.2f} cosine-similar to nearest peer hook {near_sub!r}"
                   if near else "first hook in the lane")
            print(f"   {GREEN}✓ pgvector: {subject!r} — {tag} → distinct{RESET}")
            break
        print(f"   {YELLOW}✗ pgvector: {subject!r} is {sim:.2f} similar to {near_sub!r} "
              f"(> {SIM_REJECT}) — too close, rethinking…{RESET}")
        await set_activity(session, me["id"], "rephrasing",
                           f"“{subject}” was {sim:.2f}≈ “{near_sub}” — too close, rethinking")
        avoid = (f"Your previous idea \"{subject}\" was too similar (cosine {sim:.2f}) to the "
                 f"existing hook \"{near_sub}\". Choose a materially different angle. ")
    else:
        print(f"   {YELLOW}kept last candidate after {DIVERSIFY_TRIES} tries (sim {sim:.2f}){RESET}")

    await set_activity(session, me["id"], "posting", f"“{subject}” · {sim:.2f} distinct")
    performance = random.randint(40, 100)   # simulated engagement (posting itself is simulated)
    print(f"   {BOLD}POST{RESET}: {color}{subject}{RESET} {DIM}@ {when}{RESET}  ·  "
          f"sim {sim:.2f}  ·  perf {performance}/100")
    print(f'   {DIM}"{text}"  — {reason}{RESET}')

    # memory + durable record + state + coordinate, all via MCP
    await store_hook(session, me["id"], subject, vec, performance)
    await store_post(session, me["id"], subject, text, performance, near_acct, when)
    await pg_write(session,
        f"INSERT INTO events (account_id, type, topic) VALUES ('{sql_str(me['id'])}', 'post', '{POSTS}')")
    await produce(session, POSTS, [{"key": {"account_id": me["id"]}, "value": {
        "account_id": me["id"], "persona": me["persona"], "genre": me["genre"], "type": "post",
        "subject": subject, "text": text, "scheduled_for": when, "performance": performance,
        "diversified_from": near_acct, "reason": reason, "ts": now_iso()}}])
    step(f"wrote hooks+posts+events rows · produced to '{POSTS}'")
    return {"account_id": me["id"], "subject": subject, "scheduled_for": when, "performance": performance}

# ----------------------------------------------------------------------------- trend detect + amplify
async def detect_and_amplify(session, since_db):
    banner(BLUE, "≈ detect", "clustering this round's hooks in pgvector to find a resonating theme…")
    # one pgvector query: rank this round's hooks by similarity to the top performer (the leader)
    rows = await pg_read(session,
        f"WITH recent AS (SELECT id, account_id, subject, performance, embedding FROM hooks "
        f"WHERE ts >= '{since_db}'), "
        f"leader AS (SELECT * FROM recent ORDER BY performance DESC NULLS LAST, id DESC LIMIT 1) "
        f"SELECT r.account_id, r.subject, r.performance, "
        f"round((1 - (r.embedding <=> l.embedding))::numeric, 3) AS sim_to_leader "
        f"FROM recent r, leader l ORDER BY sim_to_leader DESC, r.performance DESC")
    if not rows:
        print("   (no posts this round)")
        return None

    leader = rows[0]                         # sim_to_leader = 1.0
    theme, leader_acct = leader["subject"], leader["account_id"]
    members = [r for r in rows if float(r["sim_to_leader"]) >= THEME_SIM]
    perfs = [int(r["performance"]) for r in members if r["performance"] is not None]
    avg_perf = round(sum(perfs) / len(perfs)) if perfs else int(leader["performance"] or 0)

    step(f"leader: {BOLD}{leader_acct}{RESET} — \"{theme}\" (performance {leader['performance']}/100)")
    step(f"theme cluster: {len(members)} hook(s) within {THEME_SIM} cosine → avg performance {avg_perf}/100")
    for r in members:
        print(f"      {DIM}{r['account_id']:8} sim={r['sim_to_leader']} perf={r['performance']}  {r['subject']}{RESET}")

    trend_sig = {"type": "trend", "theme": theme, "leader": leader_acct,
                 "members": [r["account_id"] for r in members], "avg_performance": avg_perf, "ts": now_iso()}
    await produce(session, SIGNALS, [{"key": {"theme": theme}, "value": trend_sig}])
    await store_signal(session, "trend", trend_sig)
    step(f"emitted trend signal to kafka topic '{SIGNALS}' (+ durable copy)")

    # amplify: the theme owner allocates ad budget and doubles down (legit marketing ops)
    me = await load_account(session, leader_acct)
    budget = avg_perf * 5    # simulated ad spend, scales with how strongly the theme resonates
    await set_activity(session, leader_acct, "amplifying", f"${budget} behind “{theme}”")
    d = await asyncio.to_thread(
        decide, persona_system(me),
        f"A theme you own is resonating across the fleet: \"{theme}\" (avg performance "
        f"{avg_perf}/100). You're putting ${budget} of ad budget behind it to double down. "
        "Write a short amplified follow-up post and a one-line strategy. Reply with ONLY JSON: "
        '{"text": "<amplified post, max ~180 chars>", "reason": "<one line: the doubling-down play>"}')
    text, reason = d.get("text", "").strip(), d.get("reason", "").strip()

    banner(GREEN, "💰 amplify", f"{leader_acct} doubles down on the resonating theme")
    print(f"   theme:  {BOLD}{theme}{RESET}  {DIM}(avg perf {avg_perf}/100){RESET}")
    print(f"   budget: {GREEN}${budget}{RESET} {DIM}(simulated ad spend){RESET}")
    print(f'   post:   {GREEN}"{text}"{RESET}')
    print(f"   why:    {DIM}{reason}{RESET}")

    await pg_write(session,
        f"INSERT INTO events (account_id, type, topic) VALUES ('{sql_str(leader_acct)}', 'amplify', '{SIGNALS}')")
    await store_post(session, leader_acct, theme, text, avg_perf, None, "now (amplified)")
    amp_sig = {"type": "amplify", "account_id": leader_acct, "theme": theme, "budget_usd": budget,
               "text": text, "reason": reason, "ts": now_iso()}
    await produce(session, SIGNALS, [{"key": {"account_id": leader_acct}, "value": amp_sig}])
    await store_signal(session, "amplify", amp_sig)
    step(f"wrote amplify event + durable posts/signal rows + produced to '{SIGNALS}'")
    return {"theme": theme, "leader": leader_acct, "budget": budget, "avg_performance": avg_perf}

# ----------------------------------------------------------------------------- ledger
async def show_ledger(session, since_db):
    banner(GREEN, "✓ round complete", "— this round's events ledger (state in Postgres):")
    rows = await pg_read(session,
        f"SELECT account_id, type, topic, to_char(ts, 'HH24:MI:SS') AS ts FROM events "
        f"WHERE ts >= '{since_db}' ORDER BY id")
    print(f"   {DIM}{'account':10} {'type':10} {'topic':9} time{RESET}")
    for r in rows:
        print(f"   {r['account_id']:10} {r['type']:10} {r['topic']:9} {r['ts']}")

# ----------------------------------------------------------------------------- run the swarm
async def do_demo(session):
    await setup(session)
    since_iso = now_iso()                                       # scope the round (kafka ts)
    since_db = (await pg_read(session, "SELECT now() AS t"))[0]["t"]   # scope the round (pg ts)

    banner(YELLOW, "≈ round", f"{FLEET_SIZE} agents post in turn, each diversifying + staggering vs the others")
    for i, (acct_id, _) in enumerate(ROSTER[:FLEET_SIZE]):
        await agent_post(session, acct_id, PALETTE[i % len(PALETTE)], since_iso, since_db)
        await asyncio.sleep(STAGGER_SECONDS)                    # stagger → gentle on rate limits

    summary = await detect_and_amplify(session, since_db)
    await show_ledger(session, since_db)
    banner(GREEN, "✓ done", "no controller, no human, no backend — the fleet coordinated itself via Aiven.")
    return {"agents": FLEET_SIZE, "at": since_iso, **(summary or {})}

# ----------------------------------------------------------------------------- onboard (Claude Agent SDK)
# THE onboarding path: a REAL autonomous agent (claude-agent-sdk) decides to call the Aiven
# MCP provisioning tools ITSELF — instead of our Python parsing an LLM JSON decision and
# calling them for it. Used by the --onboard flag AND embedded in the autonomous conductor.
# Hybrid by design:
#   [1] run_sdk_provision() — the SDK agent autonomously creates its own Kafka lane
#       (posts.<seg>), inserts its own accounts row, and announces on the lane. We watch
#       every tool it picks and print it (that IS the autonomy demo).
#   [2] sdk_join()          — then we REUSE the deterministic, measured diversify-join via
#       the existing agent_post(), because the pgvector cosine<0.80 threshold loop must stay
#       hand-rolled + exact. onboard_sdk() wraps both for the CLI; conductor_cycle() calls
#       run_sdk_provision()+sdk_join() directly on its own session.
# claude-agent-sdk is an OPTIONAL dep, imported lazily so the rest of murmur runs without
# it; it shells out to the `claude` CLI (Node), so a missing CLI is reported with a hint.
def _seg_identity(segment):
    """Normalize a segment to (seg, acct_id, lane). We FIX the agent's identity
    (acct_<seg>, posts.<seg>) rather than let it invent one — so the deterministic join in
    step [2] knows exactly which account/lane to use, the dashboard/roster stay coherent,
    and re-runs are idempotent (ON CONFLICT)."""
    seg = re.sub(r"[^a-z0-9]+", "-", segment.strip().lower()).strip("-") or "segment"
    acct_id = f"acct_{seg.replace('-', '_')}"
    lane = f"{POSTS}.{seg}"
    return seg, acct_id, lane


async def run_sdk_provision(segment, token):
    """Let a REAL claude-agent-sdk agent provision this segment's place in the swarm by
    autonomously calling the Aiven MCP tools itself. Returns
    {acct_id, lane, persona, tools_called:[...], result_text, mcp_connected}.

    We pin the agent's identity (acct_<seg> / posts.<seg>) and hand it the exact table
    shape + tool names so it stays on-script; max_turns + a tight allowed_tools allowlist
    + bypassPermissions keep it bounded and headless. onboard_sdk() verifies the result."""
    # guarded, lazy import: keep the rest of murmur.py runnable without the SDK
    try:
        from claude_agent_sdk import (
            query, ClaudeAgentOptions,
            SystemMessage, AssistantMessage, ResultMessage,
            ToolUseBlock, TextBlock, CLINotFoundError,
        )
    except ImportError:
        raise SystemExit(
            f"{YELLOW}--onboard-sdk needs the Claude Agent SDK.{RESET}\n"
            "  pip install claude-agent-sdk   (or add --with claude-agent-sdk to uv run)\n"
            "  It also needs Node + the Claude Code CLI:\n"
            "    npm install -g @anthropic-ai/claude-code\n"
            "  (ANTHROPIC_API_KEY is already required by murmur; the SDK reads it.)")

    seg, acct_id, lane = _seg_identity(segment)

    banner(GREEN, "✦ onboard-sdk",
           f"a REAL agent (claude-agent-sdk) provisions the '{segment}' segment by "
           f"calling the Aiven MCP itself")
    step(f"identity we assign it: {GREEN}{acct_id}{RESET} · lane {BOLD}{lane}{RESET} "
         f"{DIM}(fixed by us so the deterministic join can find it){RESET}")

    # Remote Aiven MCP over HTTP with the bearer token (same endpoint as connect_and_run).
    # Its tools are exposed to the agent as mcp__aiven__<tool>.
    options = ClaudeAgentOptions(
        model=MODEL,
        mcp_servers={
            "aiven": {
                "type": "http",
                "url": MCP_URL,
                "headers": {"Authorization": f"Bearer {token}"},
            }
        },
        # tight allowlist: ONLY the provisioning tools (+ pg_read so it can self-verify).
        allowed_tools=[
            "mcp__aiven__aiven_kafka_topic_create",
            "mcp__aiven__aiven_pg_write",
            "mcp__aiven__aiven_kafka_topic_message_produce",
            "mcp__aiven__aiven_pg_read",
        ],
        # headless auto-approve. SDK 0.1.x has NO "dontAsk" mode; valid literals are
        # default/acceptEdits/plan/bypassPermissions. IMPORTANT: under bypassPermissions the
        # allow list only PRE-APPROVES — the whole mcp__aiven__* catalog is still reachable —
        # so the real guard is the DENY list below (deny wins even under bypass). We block the
        # built-in fs/shell tools AND every destructive/costly Aiven tool, so even if the agent
        # went off-script the worst it can reach are the harmless provisioning + read tools.
        permission_mode="bypassPermissions",
        disallowed_tools=[
            "Bash", "Read", "Write", "Edit", "WebFetch", "WebSearch",
            "mcp__aiven__aiven_kafka_topic_delete", "mcp__aiven__aiven_kafka_topic_update",
            "mcp__aiven__aiven_service_create", "mcp__aiven__aiven_service_update",
            "mcp__aiven__aiven_service_integration_create",
            "mcp__aiven__aiven_service_integration_update",
            "mcp__aiven__aiven_service_integration_delete",
            "mcp__aiven__aiven_pg_bouncer_create", "mcp__aiven__aiven_pg_bouncer_update",
            "mcp__aiven__aiven_pg_bouncer_delete",
            "mcp__aiven__aiven_kafka_connect_delete_connector",
            "mcp__aiven__aiven_kafka_connect_edit_connector",
            "mcp__aiven__aiven_application_deploy", "mcp__aiven__aiven_application_redeploy",
        ],
        max_turns=12,          # bounds the agent's tool-call loop (3 provisions + slack)
        setting_sources=[],    # don't load the user's ~/.claude project config
        system_prompt=(
            f"You are {acct_id}, a brand-new autonomous account-agent joining an existing "
            f"'{GENRE}' book-marketing fleet to cover the '{segment}' audience segment. "
            f"You coordinate over Aiven Kafka and remember in Aiven Postgres, and you must "
            f"PROVISION YOUR OWN PLACE in the swarm using ONLY the Aiven MCP tools — no human "
            f"will do it for you. Work concisely and in-character, then stop."),
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )

    # The task: give the agent its fixed identity and the EXACT tool args/table shape so a
    # single clean pass provisions correctly (and re-runs stay idempotent).
    prompt = (
        f"Provision your place in the swarm, in this order, using the Aiven MCP. "
        f"Project is \"{PROJECT}\", Kafka service \"{KAFKA_SVC}\", Postgres service \"{PG_SVC}\".\n\n"
        f"1) Create YOUR OWN Kafka lane topic. Call aiven_kafka_topic_create with "
        f"topic_name=\"{lane}\", partitions=1, replication=1. If it already exists, that is fine — continue.\n\n"
        f"2) Insert YOUR OWN account row. Call aiven_pg_write with this exact query "
        f"(invent a short in-character persona string for the '{segment}' segment, "
        f"single-quoted, no apostrophes inside):\n"
        f"   INSERT INTO accounts (id, persona, genre) VALUES "
        f"('{acct_id}', '<your persona>', '{GENRE}') "
        f"ON CONFLICT (id) DO UPDATE SET persona = EXCLUDED.persona, genre = EXCLUDED.genre\n\n"
        f"3) Announce your birth on YOUR lane. Call aiven_kafka_topic_message_produce with "
        f"topic_name=\"{lane}\", format=\"{FORMAT}\", and records being a one-element list "
        f"[{{\"key\": {{\"account_id\": \"{acct_id}\"}}, \"value\": {{\"account_id\": \"{acct_id}\", "
        f"\"type\": \"announce\", \"segment\": \"{segment}\", \"text\": \"<a one-line birth announcement>\", "
        f"\"ts\": \"{now_iso()}\"}}}}].\n\n"
        f"When all three succeed, reply with one short line stating your persona and that "
        f"{acct_id} is provisioned and live. Do not call any other tools.")

    tools_called, persona, result_text, mcp_ok = [], None, None, False

    # async for (never break — avoids the SDK's asyncio cleanup issues)
    try:
        async for message in query(prompt=prompt, options=options):
            if isinstance(message, SystemMessage):
                if message.subtype == "init":
                    # surface MCP connection status so a bad token/endpoint is obvious
                    for srv in (message.data or {}).get("mcp_servers", []):
                        ok = srv.get("status") == "connected"
                        mcp_ok = mcp_ok or ok
                        col = GREEN if ok else YELLOW
                        step(f"{col}MCP server {srv.get('name')}: {srv.get('status')}{RESET}")
            elif isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, ToolUseBlock):
                        # THE AUTONOMY, made visible: the agent itself chose this tool.
                        name = block.name.split("__")[-1]
                        tools_called.append(name)
                        arg_hint = (block.input.get("topic_name")
                                    or (str(block.input.get("query", ""))[:60])
                                    or "")
                        print(f"   {MAGENTA}⚡ agent → {name}{RESET} {DIM}{arg_hint}{RESET}")
                    elif isinstance(block, TextBlock) and block.text.strip():
                        print(f"   {DIM}{block.text.strip()[:140]}{RESET}")
            elif isinstance(message, ResultMessage):
                result_text = message.result
                cost = (f"${message.total_cost_usd:.4f}"
                        if message.total_cost_usd is not None else "n/a")
                tag = GREEN if not message.is_error else YELLOW
                step(f"{tag}agent finished: {message.subtype} · "
                     f"{message.num_turns} turns · {cost}{RESET}")
    except CLINotFoundError:
        raise SystemExit(
            f"{YELLOW}claude-agent-sdk is installed but the Claude Code CLI is missing.{RESET}\n"
            "  install Node 18+, then:  npm install -g @anthropic-ai/claude-code")

    if persona is None:   # the agent's row is the source of truth; this is only a fallback label
        persona = f"{segment} specialist"
    return {"acct_id": acct_id, "lane": lane, "persona": persona,
            "tools_called": tools_called, "result_text": result_text, "mcp_connected": mcp_ok}


async def sdk_join(session, segment, prov):
    """Deterministic diversify-join for an SDK-provisioned agent, on an EXISTING raw MCP session.
    Verifies (and backfills) the accounts row the agent should have written, then runs the same
    hand-rolled agent_post() pgvector cosine<0.80 loop as a normal round. Reused by BOTH the CLI
    path (onboard_sdk) and the autonomous conductor (conductor_cycle), so the measured threshold
    loop stays exact in either entry point."""
    seg, acct_id, lane = _seg_identity(segment)
    await ensure_schema(session)   # tolerate a fresh DB; idempotent

    # VERIFY the agent actually provisioned (defend against an off-script agent):
    rows = await pg_read(session,
        f"SELECT id, persona, genre FROM accounts WHERE id = '{sql_str(acct_id)}'")
    if not rows:
        step(f"{YELLOW}agent row {acct_id} not found — backfilling so the join can proceed{RESET}")
        await pg_write(session,
            f"INSERT INTO accounts (id, persona, genre) VALUES "
            f"('{sql_str(acct_id)}', '{sql_str(prov['persona'])}', '{GENRE}') "
            f"ON CONFLICT (id) DO UPDATE SET persona = EXCLUDED.persona, genre = EXCLUDED.genre")
    else:
        step(f"{GREEN}verified: agent's accounts row {acct_id} is present "
             f"(persona: {str(rows[0]['persona'])[:48]}…){RESET}")

    banner(GREEN, "↳ join",
           f"{acct_id} joins the shared '{POSTS}' round and diversifies vs the fleet "
           f"(deterministic, raw MCP)")
    await set_activity(session, acct_id, "joining", "diversifying vs the fleet")
    # wide 24h window so onboarding diversifies against the day's fleet activity whenever it runs
    window = datetime.now(timezone.utc) - timedelta(hours=24)
    since_iso = window.strftime("%Y-%m-%dT%H:%M:%SZ")
    since_db = (await pg_read(session, "SELECT (now() - interval '24 hours') AS t"))[0]["t"]
    post = await agent_post(session, acct_id, GREEN, since_iso, since_db)

    banner(GREEN, "✓ born (sdk)",
           f"{acct_id}: a real agent provisioned it via the MCP, then it posted — "
           f"no human, no backend.")
    return {"acct_id": acct_id, "lane": lane, "persona": prov["persona"],
            "tools_called": prov["tools_called"], "first_post": post}


async def onboard_sdk(token, segment):
    """THE onboarding path (CLI entry, --onboard): [1] a real claude-agent-sdk agent autonomously
    provisions its Kafka lane + accounts row + birth-announce via the MCP, then [2] the deterministic
    diversify-join (sdk_join) on a fresh raw MCP session. The autonomous conductor reuses the same two
    steps directly on its own session (see conductor_cycle)."""
    # [1] autonomous provisioning by the SDK agent (its own asyncio-driven query loop)
    prov = await run_sdk_provision(segment, token)
    # [2] deterministic join on a RAW MCP session opened via connect_and_run
    return await connect_and_run(token, lambda s: sdk_join(s, segment, prov))

# ----------------------------------------------------------------------------- tier 2 (self-optimize)
async def tier2_optimize(session):
    """Capacity rehearsal: the platform-watcher agent seeds a throwaway bench to projected scale,
    observes the swarm's own DB load via the MCP, decides + applies a pgvector index, and verifies a
    REAL seq->index speedup + recall — all via MCP, never touching the live `hooks` table."""
    await ensure_schema(session)
    bench = f"hooks_bench_{int(time.time()) % 1000000}"     # fresh per run (can't DROP via MCP)
    idx = f"{bench}_hnsw"
    banner(BLUE, "⚙ tier2", f"self-optimization rehearsal on a throwaway bench ({bench})")

    # 1) seed the bench to projected scale (synthetic 384-dim vectors), in <30s chunks
    await pg_write(session,
        f"CREATE TABLE IF NOT EXISTS {bench} (id bigserial PRIMARY KEY, account_id text NOT NULL, "
        f"subject text NOT NULL, embedding vector(384) NOT NULL, performance int, "
        f"ts timestamptz NOT NULL DEFAULT now())")
    seeded = 0
    while seeded < TIER2_ROWS:
        n = min(TIER2_CHUNK, TIER2_ROWS - seeded)
        await pg_write(session,
            f"INSERT INTO {bench} (account_id, subject, embedding, performance, ts) "
            f"SELECT 'loadgen', 'b'||gs, (SELECT array_agg(random())::vector "
            f"FROM generate_series(1,384) d WHERE gs = gs), (random()*100)::int, now() "
            f"FROM generate_series(1,{n}) gs")
        seeded += n
    await pg_write(session, f"ANALYZE {bench}")
    step(f"seeded {seeded:,} synthetic hooks to bench (projected scale)")

    # 2) OBSERVE — read the swarm's own load through the MCP
    banner(BLUE, "≈ observe", "reading our own DB load via the MCP")
    for q in await query_stats(session, limit=3, search="account_id <>"):   # the live diversify NN family
        qt = (q.get("query") or "")[:58].replace("\n", " ")
        step(f"{DIM}hot query: {q.get('calls')}× total {round(float(q.get('total_time', 0)), 1)}ms  {qt}…{RESET}")
    try:
        await db_metrics(session)
        step("fetched DB metrics (CPU/load) via aiven_service_metrics_fetch")
    except Exception as e:
        step(f"{DIM}metrics_fetch skipped ({str(e)[:40]}){RESET}")
    nn = f"SELECT id FROM {bench} ORDER BY embedding <=> {PROBE} LIMIT 10"
    _, before_ms, before_scan = await explain(session, nn)
    exact = await topk_ids(session, bench)                  # exact top-10 (no index yet → seq scan)
    step(f"baseline: {before_scan} @ {seeded:,} rows → {YELLOW}{before_ms:.1f} ms{RESET}")

    # 3) DECIDE — the watcher (LLM) chooses the remediation
    print(f"   {DIM}thinking with {MODEL}…{RESET}")
    d = await asyncio.to_thread(
        decide,
        "You are Murmur's platform-watcher agent — an autonomous DataOps agent that keeps the swarm's "
        "Aiven Postgres fast. Answer with a single JSON object only.",
        f"The fleet's hottest query is the diversify nearest-neighbour search over the `hooks` pgvector "
        f"column (cosine `<=>`, column `embedding vector(384)`). At {seeded} rows EXPLAIN shows a "
        f"{before_scan} taking {before_ms:.0f} ms. You cannot tune probes/ef_search at query time, so "
        f"favour the index with the best recall at default settings. Reply ONLY JSON: "
        '{"diagnosis": "<one line>", "index_type": "hnsw" or "ivfflat", "reason": "<why this index + cosine opclass>"}')
    itype = d.get("index_type", "hnsw").strip().lower()
    if itype not in ("hnsw", "ivfflat"):                    # guardrail: only known-safe index types
        itype = "hnsw"
    print(f"   decision: {BOLD}CREATE {itype.upper()} INDEX{RESET} (embedding, vector_cosine_ops)")
    print(f"   diagnosis: {DIM}{d.get('diagnosis', '')}{RESET}")
    print(f"   why:       {DIM}{d.get('reason', '')}{RESET}")
    try:                                                    # advisory AI cross-check (may not model pgvector)
        acct = await account_id(session)
        if acct:
            rec = json.dumps(await optimize_query(session, acct, nn))[:130]
            step(f"{DIM}EverSQL cross-check: {rec}…{RESET}")
    except Exception as e:
        step(f"{DIM}optimize_query skipped ({str(e)[:40]}){RESET}")

    # 4) ACT — apply the index via the MCP (guarded DDL: cosine opclass, unique name)
    # m=8/ef_construction=32 validated to build <30s at ~20k (the MCP write timeout); richer params
    # (m=16/ef_construction=64) exceed it at this size. Recall ~80-90% on synthetic uniform vectors.
    ddl = (f"CREATE INDEX {idx} ON {bench} USING hnsw (embedding vector_cosine_ops) "
           f"WITH (m=8, ef_construction=32)") if itype == "hnsw" else (
           f"CREATE INDEX {idx} ON {bench} USING ivfflat (embedding vector_cosine_ops) WITH (lists=100)")
    t = time.perf_counter()
    try:
        await pg_write(session, ddl)
    except Exception as e:                                  # e.g. >30s build → clean rollback, bench is throwaway
        banner(YELLOW, "⚠ tier2", f"index build failed ({str(e)[:50]}) — bench is throwaway, draining")
        await pg_write(session, f"DELETE FROM {bench}")
        return {"ok": False, "error": str(e)[:120], "bench": bench}
    build_ms = round((time.perf_counter() - t) * 1000)
    await pg_write(session, f"ANALYZE {bench}")
    step(f"built {itype} index via MCP {DIM}({build_ms} ms){RESET}; ANALYZE done")

    # 5) VERIFY — re-measure plan + timing + recall (real before/after)
    banner(BLUE, "✓ verify", "re-measuring after the agent's index")
    await pg_read(session, nn)              # warm index pages → report steady-state, not cold-start
    _, after_ms, after_scan = await explain(session, nn)
    approx = await topk_ids(session, bench)
    recall = round(len(set(approx) & set(exact)) / max(1, len(exact)) * 100)
    speedup = round(before_ms / after_ms, 1) if after_ms else None
    print(f"   {after_scan} → {GREEN}{after_ms:.1f} ms{RESET}  "
          f"(was {before_ms:.1f} ms · {BOLD}{speedup}× faster{RESET}) · recall@10 {GREEN}{recall}%{RESET}")

    # 6) EMIT — announce the optimization on the signals bus
    opt_sig = {"type": "optimize", "query": "diversify-nn", "rows": seeded, "scan_before": before_scan,
               "scan_after": after_scan, "before_ms": round(before_ms, 1), "after_ms": round(after_ms, 1),
               "speedup": speedup, "recall_pct": recall, "index": itype, "ts": now_iso()}
    await produce(session, SIGNALS, [{"key": {"type": "optimize"}, "value": opt_sig}])
    await store_signal(session, "optimize", opt_sig)
    step(f"emitted optimization signal to '{SIGNALS}' (+ durable copy)")

    # 7) CLEANUP — drain the bench (empty table+index persist, never read by the swarm)
    await pg_write(session, f"DELETE FROM {bench}")
    banner(GREEN, "✓ tier2 done",
           f"watcher self-optimized via the MCP: {before_scan} {before_ms:.0f}ms → {after_scan} "
           f"{after_ms:.0f}ms, recall {recall}% — live `hooks` untouched.")
    return {"ok": True, "bench": bench, "rows": seeded, "before_ms": round(before_ms, 1),
            "after_ms": round(after_ms, 1), "speedup": speedup, "recall_pct": recall}

async def reset_data(session, hard=False):
    """Demo reset: clear the swarm's runtime data so the wall starts EMPTY, then the agents
    refill it in real time while the judges watch. DELETE only — the MCP write tool blocks
    TRUNCATE/DROP. Keeps the `accounts` roster by default (the constellation stays populated
    and visibly 'ready to act'); `--hard` also clears accounts so the fleet self-onboards from
    zero. Same DB the live dashboard reads, so the wall goes blank the instant this finishes."""
    await ensure_schema(session)
    banner(YELLOW, "⟲ reset", "clearing the wall for a clean live demo (all via the MCP)")
    for tbl in ("signals", "events", "hooks", "posts"):   # posts before accounts (FK order)
        await pg_write(session, f"DELETE FROM {tbl}")
        step(f"cleared {tbl}")
    if hard:
        await pg_write(session, "DELETE FROM accounts")
        step("cleared accounts — the fleet will self-onboard from zero")
    banner(GREEN, "✓ reset done",
           "wall is empty — run a round (or let the conductor) and watch it fill in real time.")
    return {"reset": True, "hard": hard}

async def connect_and_run(token, runner=None):
    """Connect to the Aiven MCP (remote first, local stdio fallback) and run `runner`
    (default do_demo) exactly once on whichever transport exposes the write tools."""
    attempts = [
        ("remote https://mcp.aiven.live",
         lambda: streamablehttp_client(MCP_URL, headers={"Authorization": f"Bearer {token}"})),
        ("local stdio (./mcp-aiven)",
         lambda: stdio_client(StdioServerParameters(
             command="node", args=[LOCAL_SERVER],
             env={**os.environ, "AIVEN_TOKEN": token, "MCP_TRANSPORT": "stdio"}))),
    ]
    last_err = None
    for name, make_ctx in attempts:
        if name.startswith("local") and not os.path.exists(LOCAL_SERVER):
            last_err = f"{LOCAL_SERVER} not built (cd mcp-aiven && npm install && npm run build)"
            print(f"{DIM}[mcp] skipping local fallback: {last_err}{RESET}")
            continue
        demo_started = False
        try:
            async with make_ctx() as conn:
                read, write = conn[0], conn[1]
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    have = {t.name for t in (await session.list_tools()).tools}
                    need = {"aiven_pg_read", "aiven_pg_write",
                            "aiven_kafka_topic_message_produce", "aiven_kafka_topic_message_list"}
                    if need - have:
                        raise RuntimeError(f"endpoint missing tools {sorted(need - have)} "
                                           f"(read-only?) — trying next transport")
                    print(f"{GREEN}[mcp] connected via {name}{RESET}")
                    demo_started = True
                    return await (runner or do_demo)(session)
        except Exception as e:                       # noqa: BLE001
            if demo_started:
                raise                                # demo error: do NOT re-run on fallback
            last_err = e
            print(f"{YELLOW}[mcp] {name} unavailable: {e}{RESET}")
    raise SystemExit(f"Could not reach the Aiven MCP on any transport. Last error: {last_err}")

# ----------------------------------------------------------------------------- deploy mode (worker)
STATUS = {"service": "murmur-fleet", "fleet_size": FLEET_SIZE, "cycles": 0,
          "last_decision": None, "last_round": None, "last_error": None}

def _truthy(v):
    return str(v).strip().lower() in ("1", "true", "yes", "on")

def serve_status(port):
    """Tiny stdlib HTTP status endpoint — satisfies the Aiven app's required port and gives a
    liveness/last-round view. The real work is the background round loop in worker()."""
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            body = json.dumps({**STATUS, "ok": True}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def log_message(self, *a):   # keep the round logs clean
            return
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()

async def conductor_cycle(session, token):
    """One autonomous cycle: observe the swarm's own state via the MCP, let the LLM conductor decide
    the next move, then dispatch it. Initiative is LLM-made, not clock-driven. `token` is threaded
    through so the 'onboard' action can drive the Claude Agent SDK provisioning."""
    await ensure_schema(session)
    row = (await pg_read(session,
        "SELECT (SELECT count(*) FROM posts WHERE created_at > now()-interval '30 minutes') AS recent_posts, "
        "(SELECT count(*) FROM accounts) AS agents, (SELECT count(*) FROM hooks) AS hooks, "
        "COALESCE(round(extract(epoch FROM (now()-(SELECT max(created_at) FROM posts)))/60)::int, 999) AS mins_since_post, "
        "COALESCE(round(extract(epoch FROM (now()-(SELECT max(ts) FROM signals WHERE kind='optimize')))/60)::int, 999) AS mins_since_optimize, "
        "COALESCE((SELECT payload->>'theme' FROM signals WHERE kind='trend' ORDER BY id DESC LIMIT 1), '—') AS top_theme"))[0]
    state = {k: row.get(k) for k in ("recent_posts", "agents", "hooks", "mins_since_post", "mins_since_optimize", "top_theme")}

    banner(BLUE, "◆ conductor", "observing the swarm + deciding the next move (no human, no schedule)")
    step(f"{DIM}state: {state['agents']} agents · {state['recent_posts']} posts/30m · "
         f"{state['mins_since_post']}m since last post · {state['hooks']} hooks · theme \"{state['top_theme']}\"{RESET}")
    print(f"   {DIM}thinking with {MODEL}…{RESET}")
    d = await asyncio.to_thread(conductor_decide, state)
    action = (d.get("action") or "round").strip().lower()
    if action not in ("round", "onboard", "optimize", "idle"):
        action = "round"
    reason = d.get("reason", "").strip()
    print(f"   decision: {BOLD}{action.upper()}{RESET} — {DIM}{reason}{RESET}")
    STATUS["last_decision"] = {"action": action, "reason": reason, "ts": now_iso()}

    if action == "round":
        return await do_demo(session)
    if action == "onboard":
        # autonomous onboarding via the Claude Agent SDK: a REAL agent provisions its own lane +
        # state (its own SDK session), then we diversify-join on THIS already-open raw session.
        segment = d.get("segment") or "new-segment"
        prov = await run_sdk_provision(segment, token)
        return await sdk_join(session, segment, prov)
    if action == "optimize":
        return await tier2_optimize(session)
    step("idle — letting the stream settle")
    return {"action": "idle", "reason": reason}

async def worker(token):
    """The autonomous conductor loop: every MURMUR_INTERVAL seconds the swarm DECIDES (via the LLM
    conductor) and acts. A failed cycle must never kill the loop — log and try again next interval."""
    interval = int(os.environ.get("MURMUR_INTERVAL", "180"))
    print(f"{GREEN}[conductor] autonomous loop started — a fresh decision every ~{interval}s{RESET}")
    while True:
        try:
            STATUS["last_round"] = await connect_and_run(token, lambda s: conductor_cycle(s, token))
            STATUS["cycles"] = STATUS.get("cycles", 0) + 1
            STATUS["last_error"] = None
        except Exception as e:                       # noqa: BLE001 — keep the loop alive
            STATUS["last_error"] = str(e)[:200]
            print(f"{YELLOW}[conductor] cycle failed: {e}{RESET}")
        print(f"{DIM}[conductor] next decision in {interval}s{RESET}")
        await asyncio.sleep(interval)

def load_dotenv():
    """Minimal .env loader (no dependency): populate os.environ from a .env next to this
    script or in the cwd, without overwriting variables already set in the real environment."""
    here = os.path.dirname(os.path.abspath(__file__))
    for path in dict.fromkeys([os.path.join(here, ".env"), os.path.join(os.getcwd(), ".env")]):
        if not os.path.isfile(path):
            continue
        with open(path) as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                if line.lower().startswith("export "):
                    line = line[7:]
                key, _, val = line.partition("=")
                key, val = key.strip(), val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val

def main():
    global AC
    load_dotenv()
    missing = [v for v in ("ANTHROPIC_API_KEY", "AIVEN_TOKEN") if not os.environ.get(v)]
    if missing:
        sys.exit(f"Missing required env var(s): {', '.join(missing)}\n"
                 "  put them in a .env next to this script, or:\n"
                 "  export ANTHROPIC_API_KEY=sk-ant-...   (for claude-sonnet)\n"
                 "  export AIVEN_TOKEN=...                 (Aiven personal token, for the MCP)")
    AC = Anthropic()  # reads ANTHROPIC_API_KEY from env

    rule()
    print(f"{BOLD}  Murmur swarm — self-coordinating fleet ({FLEET_SIZE} agents){RESET}")
    print(f"{DIM}  diversify (pgvector) + stagger over kafka '{POSTS}' · trend→amplify over '{SIGNALS}' · "
          f"state in postgres · all via the Aiven MCP{RESET}")
    rule()

    token = os.environ["AIVEN_TOKEN"]
    if "--reset" in sys.argv:
        asyncio.run(connect_and_run(token, lambda s: reset_data(s, "--hard" in sys.argv)))
    elif "--onboard" in sys.argv or "--onboard-sdk" in sys.argv:
        flag = "--onboard" if "--onboard" in sys.argv else "--onboard-sdk"
        i = sys.argv.index(flag)
        segment = sys.argv[i + 1] if i + 1 < len(sys.argv) else "new-segment"
        # onboarding is SDK-driven: a real agent provisions itself, then diversify-joins.
        # onboard_sdk runs the SDK session then opens its own raw session for the join,
        # so (unlike the others) it is NOT wrapped in connect_and_run here.
        asyncio.run(onboard_sdk(token, segment))
    elif "--tier2" in sys.argv:
        asyncio.run(connect_and_run(token, tier2_optimize))
    elif "--serve" in sys.argv or _truthy(os.environ.get("MURMUR_SERVE", "")):
        port = int(os.environ.get("PORT", "8080"))
        threading.Thread(target=serve_status, args=(port,), daemon=True).start()
        print(f"{GREEN}[conductor] status endpoint on 0.0.0.0:{port} · autonomous — the swarm decides "
              f"its own move every {os.environ.get('MURMUR_INTERVAL', '180')}s{RESET}")
        asyncio.run(worker(token))
    else:
        asyncio.run(connect_and_run(token))
    rule()

if __name__ == "__main__":
    main()
