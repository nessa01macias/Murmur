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
    # first run downloads a small (~130MB) local embedding model, one time

Transport: connects to the hosted Aiven MCP (https://mcp.aiven.live/mcp) using AIVEN_TOKEN
as a bearer; if that endpoint lacks the write tools (read-only), it falls back to spawning
the bundled local server (./mcp-aiven, built with `npm install && npm run build`) over stdio.

Deploy (step 4): `python murmur.py --serve` runs it as a long-running worker — a status
endpoint on $PORT (default 8080) plus a coordination round every $MURMUR_INTERVAL seconds
(default 300). The Dockerfile builds exactly this; deploy via Aiven Apps with ANTHROPIC_API_KEY
and AIVEN_TOKEN injected as secrets — still all via the MCP, no direct DB/Kafka drivers.
"""

import asyncio
import json
import os
import random
import re
import sys
import threading
from datetime import datetime, timezone
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
    """Pull a JSON value out of an MCP/LLM text payload, tolerant of wrappers,
    prose, or markdown code fences around it."""
    if text is None:
        raise ValueError("empty tool/LLM result")
    m = re.search(r"<untrusted-[^>]*>(.*)</untrusted-[^>]*>", text, re.DOTALL)
    if m:
        text = m.group(1)
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\n?|\n?```$", "", text).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    starts = [i for i in (text.find("{"), text.find("[")) if i != -1]
    if not starts:
        raise ValueError(f"no JSON found in: {text[:200]!r}")
    start = min(starts)
    for end in range(len(text), start, -1):
        try:
            return json.loads(text[start:end])
        except json.JSONDecodeError:
            continue
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
            "service unavailable", "503", "timed out", "timeout"))
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
        "partitions": {"0": {"offset": offset}}, "format": FORMAT, "timeout": 10000})
    parsed = extract_json(txt)
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict):
        for k in ("messages", "records", "data"):
            if isinstance(parsed.get(k), list):
                return parsed[k]
    return []

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

# ----------------------------------------------------------------------------- setup (idempotent)
async def setup(session):
    banner(YELLOW, "[setup]", "ensuring tables, fleet, topics, pgvector (all via MCP)")
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
    for acct_id, persona in ROSTER[:FLEET_SIZE]:
        await pg_write(session,
            f"INSERT INTO accounts (id, persona, genre) VALUES "
            f"('{acct_id}', '{sql_str(persona)}', '{GENRE}') "
            f"ON CONFLICT (id) DO UPDATE SET persona = EXCLUDED.persona, genre = EXCLUDED.genre")
    for topic in (POSTS, SIGNALS):
        try:  # topics are usually pre-created; tolerate "already exists"
            await call(session, "aiven_kafka_topic_create", {
                "project": PROJECT, "service_name": KAFKA_SVC,
                "topic_name": topic, "partitions": 1, "replication": 1})
        except Exception:
            pass
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

    # read the Kafka stream (the coordination bus) to see what the fleet just planned
    peers = []
    for _ in range(8):
        try:
            recs = await consume(session, POSTS, 0)
        except Exception as e:
            step(f"{DIM}stream not ready ({str(e)[:40]}…), retrying{RESET}")
            await asyncio.sleep(2)
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
        avoid = (f"Your previous idea \"{subject}\" was too similar (cosine {sim:.2f}) to the "
                 f"existing hook \"{near_sub}\". Choose a materially different angle. ")
    else:
        print(f"   {YELLOW}kept last candidate after {DIVERSIFY_TRIES} tries (sim {sim:.2f}){RESET}")

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

    await produce(session, SIGNALS, [{"key": {"theme": theme}, "value": {
        "type": "trend", "theme": theme, "leader": leader_acct,
        "members": [r["account_id"] for r in members], "avg_performance": avg_perf, "ts": now_iso()}}])
    step(f"emitted trend signal to kafka topic '{SIGNALS}'")

    # amplify: the theme owner allocates ad budget and doubles down (legit marketing ops)
    me = await load_account(session, leader_acct)
    budget = avg_perf * 5    # simulated ad spend, scales with how strongly the theme resonates
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
    await produce(session, SIGNALS, [{"key": {"account_id": leader_acct}, "value": {
        "type": "amplify", "account_id": leader_acct, "theme": theme, "budget_usd": budget,
        "text": text, "reason": reason, "ts": now_iso()}}])
    step(f"wrote amplify event + durable posts row + produced to '{SIGNALS}'")
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

async def connect_and_run(token):
    """Connect to the Aiven MCP (remote first, local stdio fallback) and run the demo
    exactly once on whichever transport exposes the write tools."""
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
                    return await do_demo(session)
        except Exception as e:                       # noqa: BLE001
            if demo_started:
                raise                                # demo error: do NOT re-run on fallback
            last_err = e
            print(f"{YELLOW}[mcp] {name} unavailable: {e}{RESET}")
    raise SystemExit(f"Could not reach the Aiven MCP on any transport. Last error: {last_err}")

# ----------------------------------------------------------------------------- deploy mode (worker)
STATUS = {"service": "murmur-fleet", "fleet_size": FLEET_SIZE, "rounds": 0,
          "last_round": None, "last_error": None}

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

async def worker(token):
    """Run coordination rounds forever, every MURMUR_INTERVAL seconds, updating STATUS.
    A failed round must never kill the worker — it logs and tries again next interval."""
    interval = int(os.environ.get("MURMUR_INTERVAL", "300"))
    while True:
        try:
            STATUS["last_round"] = await connect_and_run(token)
            STATUS["rounds"] += 1
            STATUS["last_error"] = None
        except Exception as e:                       # noqa: BLE001 — keep the worker alive
            STATUS["last_error"] = str(e)[:200]
            print(f"{YELLOW}[worker] round failed: {e}{RESET}")
        print(f"{DIM}[worker] sleeping {interval}s until next round{RESET}")
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
    if "--serve" in sys.argv or _truthy(os.environ.get("MURMUR_SERVE", "")):
        port = int(os.environ.get("PORT", "8080"))
        threading.Thread(target=serve_status, args=(port,), daemon=True).start()
        print(f"{GREEN}[worker] status endpoint on 0.0.0.0:{port} · round every "
              f"{os.environ.get('MURMUR_INTERVAL', '300')}s{RESET}")
        asyncio.run(worker(token))
    else:
        asyncio.run(connect_and_run(token))
    rule()

if __name__ == "__main__":
    main()
