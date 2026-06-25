"""
Murmur dashboard — data layer (READ-ONLY).

This is the ONE place the dashboard touches Postgres, and it only ever SELECTs.
It connects DIRECTLY to pg-conductor with psycopg (NOT through the Aiven MCP):
the dashboard is a human-facing viewer, not an agent, so it has no business holding
Aiven OAuth credentials. A plain read-only Postgres connection is the correct seam.

Connection string comes from $DATABASE_URL — never hardcoded, never logged. For Aiven
the URL must enable TLS, e.g.  postgres://...:.../defaultdb?sslmode=require

Mode resolution (per request, cheap):
  MURMUR_MOCK truthy      -> full mock, no DB needed (preview the UI with zero config)
  else DATABASE_URL set   -> LIVE: real accounts; real posts if present, else mock posts
  else                    -> full mock

Every section reports its own source ("live" | "derived" | "mock …") so the UI can
label exactly what's real vs theatrical. Nothing here ever writes.

----------------------------------------------------------------------------------
SCHEMA CONTRACT — what this dashboard reads (have the agent session match these,
or rename in the *_CANDS lists below; column detection is tolerant of aliases):

  accounts(id text, persona text, genre text)                       [exists today]

  posts(                                                            [agent build adds]
    id            bigserial primary key,
    account_id    text references accounts(id),
    subject       text,        -- the hook / 2-5 word topic
    body          text,        -- the post text
    performance   numeric,     -- engagement signal (drives the amplify strip)
    diversified_from text,     -- peer account this was staggered/diversified against
    scheduled_for text,        -- agent's chosen post time
    created_at    timestamptz default now())

  Optional budget/theme source for the operator strip (else it's derived from
  posts.performance): a table named budget|themes(theme text, budget numeric,
  momentum numeric).

Until `posts` exists with rows, the feed/operator strip render against mock rows
that are wired through the REAL account ids, so tiles still correspond to real
accounts. Flip to live by creating `posts` and inserting — no dashboard change.
----------------------------------------------------------------------------------
"""

import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

# --- column aliases we accept for each logical field (first match wins) -----------
ACCOUNT_CANDS = ["account_id", "account", "acct_id", "acct"]
SUBJECT_CANDS = ["subject", "hook", "topic", "headline", "title"]
BODY_CANDS    = ["body", "text", "content", "post", "message"]
PERF_CANDS    = ["performance", "score", "engagement", "perf", "metric", "reach"]
CREATED_CANDS = ["created_at", "ts", "timestamp", "posted_at", "created", "time"]
DIVERSE_CANDS = ["diversified_from", "diversify_from", "staggered_from", "peer", "vs_account"]
SCHED_CANDS   = ["scheduled_for", "scheduled", "post_time", "scheduled_at"]

POSTS_TABLES  = ["posts"]                       # preferred richer table
BUDGET_TABLES = ["budget", "themes", "ad_budget", "allocations"]

MYSTERY_HOOKS = [
    "locked-room twist", "unreliable narrator", "slow-burn reveal", "red-herring trap",
    "midnight confession", "the detective's blind spot", "a clue in the margins",
]


# --------------------------------------------------------------------------- helpers
def _now():
    return datetime.now(timezone.utc)


def _iso(v):
    return v.isoformat() if isinstance(v, datetime) else v


def _truthy(v):
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _mode():
    """('mock'|'live', database_url_or_None) for this request."""
    if _truthy(os.environ.get("MURMUR_MOCK", "")):
        return "mock", None
    url = os.environ.get("DATABASE_URL")
    return ("live", url) if url else ("mock", None)


def _connect(url):
    # Read-only at the server level too (belt and suspenders); short timeout so a
    # bad connection string fails fast instead of hanging the poll.
    return psycopg.connect(
        url,
        autocommit=True,
        row_factory=dict_row,
        connect_timeout=5,
        options="-c default_transaction_read_only=on -c statement_timeout=8000",
    )


def _tables(cur):
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    return {r["table_name"] for r in cur.fetchall()}


def _columns(cur, table):
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=%s",
        (table,),
    )
    return {r["column_name"] for r in cur.fetchall()}


def _pick(cols, candidates):
    for c in candidates:
        if c in cols:
            return c
    return None


def _col_as(col, alias):
    if col:
        return sql.SQL("{} AS {}").format(sql.Identifier(col), sql.Identifier(alias))
    return sql.SQL("NULL AS {}").format(sql.Identifier(alias))


# ------------------------------------------------------------------------- accounts
def _get_accounts(cur):
    """Real accounts, exactly as seeded — the tile wall auto-grows with this table."""
    cols = _columns(cur, "accounts")
    persona = "persona" if "persona" in cols else None
    genre = "genre" if "genre" in cols else None
    q = sql.SQL("SELECT id AS id, {p}, {g} FROM accounts ORDER BY id").format(
        p=_col_as(persona, "persona"), g=_col_as(genre, "genre")
    )
    cur.execute(q)
    return cur.fetchall()


def _mock_accounts():
    return [
        {"id": "acct_a", "persona": "Nova — runs a popular mystery-book account; lives for "
         "clever twists and slow-burn reveals", "genre": "mystery"},
        {"id": "acct_b", "persona": "Echo — runs a sister mystery-book account in the same "
         "fleet; keeps the feed fresh and varied", "genre": "mystery"},
    ]


# ---------------------------------------------------------------------------- posts
def _get_posts(cur, tables, limit):
    """
    Returns (rows, source). Prefers the richer `posts` table; falls back to the thin
    `events` table; otherwise mock. Rows are normalized to:
      {id, account_id, subject, body, performance, created_at, diversified_from, scheduled_for}
    """
    table = _pick(tables, POSTS_TABLES)
    if table:
        cols = _columns(cur, table)
        acc = _pick(cols, ACCOUNT_CANDS)
        crt = _pick(cols, CREATED_CANDS)
        idc = "id" if "id" in cols else None
        order = crt or idc
        q = sql.SQL("SELECT {sel} FROM {tbl}").format(
            tbl=sql.Identifier(table),
            sel=sql.SQL(", ").join([
                _col_as(idc, "id"),
                _col_as(acc, "account_id"),
                _col_as(_pick(cols, SUBJECT_CANDS), "subject"),
                _col_as(_pick(cols, BODY_CANDS), "body"),
                _col_as(_pick(cols, PERF_CANDS), "performance"),
                _col_as(crt, "created_at"),
                _col_as(_pick(cols, DIVERSE_CANDS), "diversified_from"),
                _col_as(_pick(cols, SCHED_CANDS), "scheduled_for"),
            ]),
        )
        if order:
            q = q + sql.SQL(" ORDER BY {o} DESC NULLS LAST").format(o=sql.Identifier(order))
        q = q + sql.SQL(" LIMIT %s")
        cur.execute(q, (limit,))
        rows = [_norm_post(r) for r in cur.fetchall()]
        if rows:
            return rows, "live"
        # Empty posts table. For a live demo we must NOT fall back to the theatrical mock
        # (it would masquerade as real on the wall); MURMUR_NO_MOCK shows a true empty/standby
        # state so a fresh `--reset` reads as "agents ready, awaiting first round".
        if _truthy(os.environ.get("MURMUR_NO_MOCK", "")):
            return [], "live (empty — awaiting first round)"
        ids = [a["id"] for a in _get_accounts(cur)] or ["acct_a", "acct_b"]
        return _mock_posts(ids, limit), "mock (posts table empty)"

    # graceful intermediate: thin events table (account + time only, no subject)
    if "events" in tables and _count(cur, "events") > 0:
        cur.execute(
            "SELECT id, account_id, type, ts FROM events ORDER BY ts DESC NULLS LAST LIMIT %s",
            (limit,),
        )
        rows = [{
            "id": r["id"], "account_id": r["account_id"],
            "subject": None, "body": None, "performance": None,
            "created_at": _iso(r["ts"]), "diversified_from": None, "scheduled_for": None,
        } for r in cur.fetchall()]
        return rows, "live (events; thin schema — no subject/perf yet)"

    ids = [a["id"] for a in _get_accounts(cur)] or ["acct_a", "acct_b"]
    return _mock_posts(ids, limit), "mock (no posts table yet)"


def _norm_post(r):
    perf = r.get("performance")
    return {
        "id": r.get("id"),
        "account_id": r.get("account_id"),
        "subject": r.get("subject"),
        "body": r.get("body"),
        "performance": float(perf) if perf is not None else None,
        "created_at": _iso(r.get("created_at")),
        "diversified_from": r.get("diversified_from"),
        "scheduled_for": _iso(r.get("scheduled_for")),
    }


def _count(cur, table):
    cur.execute(sql.SQL("SELECT count(*) AS n FROM {}").format(sql.Identifier(table)))
    return cur.fetchone()["n"]


def _mock_posts(account_ids, limit):
    """
    Theatrical-but-honest mock: alternating accounts, staggered times (no collisions),
    deliberately different subjects (diversify), and a rising 'unreliable narrator'
    theme so the operator strip has a real spike to amplify. Wired through REAL ids.
    """
    a = account_ids[0]
    b = account_ids[1] if len(account_ids) > 1 else account_ids[0]
    now = _now()
    # (seconds_ago, account, subject, performance, diversified_from)
    script = [
        (300, a, "locked-room twist",        140, None),
        (255, b, "unreliable narrator",      210, a),
        (205, a, "slow-burn reveal",         180, None),
        (160, b, "unreliable narrator",      430, a),
        (120, a, "red-herring trap",         160, None),
        (78,  b, "unreliable narrator",      760, a),
        (40,  a, "the detective's blind spot", 150, None),
        (12,  b, "unreliable narrator",      980, a),
    ]
    rows = []
    for i, (ago, acct, subj, perf, div) in enumerate(script):
        rows.append({
            "id": f"mock-{i}",
            "account_id": acct,
            "subject": subj,
            "body": f"[mock] {subj} — a hook for the {subj.split()[0]} crowd.",
            "performance": float(perf),
            "created_at": _iso(now - timedelta(seconds=ago)),
            "diversified_from": div,
            "scheduled_for": None,
        })
    rows.sort(key=lambda r: r["created_at"], reverse=True)
    return rows[:limit]


# ------------------------------------------------------------------- operator strip
def _get_ops(cur, tables, posts, posts_live):
    """
    The amplify view: which theme is resonating and where budget is flowing.
    Prefer a real budget/themes table; else derive from posts.performance.
    """
    budget_total = 1000

    btable = _pick(tables, BUDGET_TABLES) if cur is not None else None
    if btable:
        bcols = _columns(cur, btable)
        theme_c = _pick(bcols, ["theme", "subject", "hook", "name"])
        budget_c = _pick(bcols, ["budget", "allocation", "spend", "amount"])
        mom_c = _pick(bcols, ["momentum", "performance", "score", "weight"])
        if theme_c and budget_c:
            q = sql.SQL("SELECT {t}, {b}, {m} FROM {tbl} ORDER BY {b} DESC NULLS LAST LIMIT 6").format(
                t=_col_as(theme_c, "theme"), b=_col_as(budget_c, "budget"),
                m=_col_as(mom_c, "momentum"), tbl=sql.Identifier(btable),
            )
            cur.execute(q)
            themes = [{
                "theme": r["theme"],
                "budget": float(r["budget"]) if r["budget"] is not None else 0.0,
                "momentum": float(r["momentum"]) if r.get("momentum") is not None else None,
                "spike": i == 0,
            } for i, r in enumerate(cur.fetchall())]
            total = sum(t["budget"] for t in themes) or budget_total
            for t in themes:
                t["share"] = round(t["budget"] / total, 3)
            return {"themes": themes, "top": themes[0]["theme"] if themes else None,
                    "budget_total": int(total), "source": "live (budget table)"}

    # derive from posts performance (real if posts are live, else mock)
    mom = defaultdict(float)
    has_perf = False
    for p in posts:
        s = p.get("subject") or "—"
        perf = p.get("performance")
        if perf is not None:
            mom[s] += perf
            has_perf = True
    if not has_perf:                                # no signal — momentum by frequency
        for p in posts:
            mom[p.get("subject") or "—"] += 1.0

    ranked = sorted(mom.items(), key=lambda kv: kv[1], reverse=True)
    total = sum(v for _, v in ranked) or 1.0
    themes = []
    for i, (s, v) in enumerate(ranked[:5]):
        share = v / total
        themes.append({"theme": s, "momentum": round(v, 1), "share": round(share, 3),
                       "budget": round(budget_total * share), "spike": i == 0 and len(ranked) > 1})
    source = "derived (posts.performance)" if (posts_live and has_perf) else "mock"
    return {"themes": themes, "top": themes[0]["theme"] if themes else None,
            "budget_total": budget_total, "source": source}


def _get_signals(cur, tables, limit=14):
    """Recent autonomous signals (trend / amplify / optimize) the agents persisted to the
    `signals` table. Each row's `payload` is jsonb → already a dict via psycopg. (rows, source)."""
    if "signals" not in tables:
        return [], "none"
    cur.execute("SELECT kind, payload, ts FROM signals ORDER BY id DESC LIMIT %s", (limit,))
    rows = [{"kind": r["kind"], "payload": r["payload"], "ts": _iso(r["ts"])} for r in cur.fetchall()]
    return rows, ("live" if rows else "empty")


def _get_activity(cur, tables):
    """Each agent's CURRENT work-step (reading → thinking → rephrasing → posting → amplifying →
    provisioning), latest row per account from the agents' live work-trail. Lets the wall show the
    swarm *working*, not just its finished posts. Read-only; {} if the table isn't there yet."""
    if "activity" not in tables:
        return {}
    try:
        cur.execute(
            "SELECT DISTINCT ON (account_id) account_id, state, detail, ts "
            "FROM activity ORDER BY account_id, id DESC"
        )
        return {r["account_id"]: {"state": r["state"], "detail": r["detail"], "ts": _iso(r["ts"])}
                for r in cur.fetchall()}
    except Exception:  # noqa: BLE001 — liveness garnish; never break the poll
        return {}


def _get_diversity(cur, tables):
    """Live pgvector readout. For each hook (latest per account+subject) find the nearest
    peer hook from ANOTHER account within the same ~round window, and return the cosine
    similarity — the EXACT metric the agents diversify on, recomputed live in Postgres so
    the feed can show real numbers. Lower sim = more distinct. The 20-minute window scopes
    it to the round the agent actually diversified against (not all-time near-dupes).

    Read-only; returns {} if hooks/pgvector aren't present. Keyed by (account_id, subject)
    so it maps cleanly onto `posts` rows in get_state()."""
    if "hooks" not in tables:
        return {}
    try:
        cur.execute(
            """
            WITH h AS (
              SELECT account_id, subject, embedding, ts,
                     row_number() OVER (PARTITION BY account_id, subject ORDER BY ts DESC) AS rn
              FROM hooks
            )
            SELECT h.account_id, h.subject,
                   n.account_id AS near, n.subject AS near_subject, n.sim
            FROM h
            LEFT JOIN LATERAL (
              SELECT o.account_id, o.subject,
                     round((1 - (h.embedding <=> o.embedding))::numeric, 3)::float8 AS sim
              FROM hooks o
              WHERE o.account_id <> h.account_id
                AND o.ts BETWEEN h.ts - interval '20 minutes' AND h.ts + interval '20 minutes'
              ORDER BY h.embedding <=> o.embedding
              LIMIT 1
            ) n ON true
            WHERE h.rn = 1
            """
        )
        out = {}
        for r in cur.fetchall():
            out[(r["account_id"], r["subject"])] = {
                "near": r["near"],
                "near_subject": r["near_subject"],
                "sim": float(r["sim"]) if r["sim"] is not None else None,
            }
        return out
    except Exception:  # noqa: BLE001 — diversity is a bonus readout; never break the poll
        return {}


# ------------------------------------------------------------------------ public API
def get_state(limit=60):
    """The whole payload the dashboard polls. Never raises — on any DB error it
    degrades to mock and reports the error string so the demo never white-screens.
    (limit=60 so onboarded agents' posts aren't pushed out of the feed window.)"""
    mode, url = _mode()
    generated_at = _iso(_now())

    if mode == "mock":
        accounts = _mock_accounts()
        ids = [a["id"] for a in accounts]
        posts = _mock_posts(ids, limit)
        ops = _get_ops(None, set(), posts, posts_live=False)
        return {
            "generated_at": generated_at,
            "mode": "mock",
            "error": None,
            "accounts": accounts,
            "posts": posts,
            "ops": ops,
            "sources": {"accounts": "mock", "posts": "mock (no DATABASE_URL set)", "ops": ops["source"]},
        }

    try:
        with _connect(url) as conn, conn.cursor() as cur:
            tables = _tables(cur)
            accounts = _get_accounts(cur) if "accounts" in tables else _mock_accounts()
            accounts_src = "live" if "accounts" in tables else "mock (no accounts table)"
            posts, posts_src = _get_posts(cur, tables, limit)
            # live pgvector diversity: attach each post's real cosine to its nearest peer hook
            div = _get_diversity(cur, tables) if posts_src == "live" else {}
            for p in posts:
                d = div.get((p.get("account_id"), p.get("subject")))
                if d:
                    p["sim"], p["near"], p["near_subject"] = d["sim"], d["near"], d["near_subject"]
            ops = _get_ops(cur, tables, posts, posts_live=posts_src == "live")
            signals, signals_src = _get_signals(cur, tables)
            activity = _get_activity(cur, tables)
            return {
                "generated_at": generated_at,
                "mode": "live",
                "error": None,
                "accounts": accounts,
                "posts": posts,
                "ops": ops,
                "signals": signals,
                "activity": activity,
                "sources": {"accounts": accounts_src, "posts": posts_src,
                            "ops": ops["source"], "signals": signals_src,
                            "diversity": "live (pgvector)" if div else "none",
                            "activity": "live" if activity else "none"},
            }
    except Exception as e:  # noqa: BLE001 — viewer must stay up; show degraded state
        accounts = _mock_accounts()
        ids = [a["id"] for a in accounts]
        posts = _mock_posts(ids, limit)
        ops = _get_ops(None, set(), posts, posts_live=False)
        return {
            "generated_at": generated_at,
            "mode": "mock",
            "error": f"{type(e).__name__}: {e}",
            "accounts": accounts,
            "posts": posts,
            "ops": ops,
            "sources": {"accounts": "mock (db error)", "posts": "mock (db error)", "ops": ops["source"]},
        }
