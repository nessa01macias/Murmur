"""
Murmur dashboard — tiny Flask app that makes the swarm visible.

  GET /            -> the single-page wall (tiles + live feed + operator strip)
  GET /api/state   -> JSON the page polls every ~2s and re-renders from

Read-only viewer. It reads pg-conductor directly via data.py (psycopg), never
through the Aiven MCP, and never writes. Run it:

    cd dashboard
    pip install -r requirements.txt
    # preview with zero config (full mock):
    MURMUR_MOCK=1 python app.py
    # or go live (Aiven needs TLS in the URL):
    export DATABASE_URL='postgres://USER:PASS@HOST:PORT/defaultdb?sslmode=require'
    python app.py

Then open http://127.0.0.1:5050
"""

import os

from flask import Flask, jsonify, render_template

try:  # optional: load a local .env if python-dotenv is installed
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except Exception:  # noqa: BLE001
    pass

import data

app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/state")
def state():
    return jsonify(data.get_state())


if __name__ == "__main__":
    # Local default: loopback + auto-reload. In a container (Hugging Face Spaces etc.) set
    # HOST=0.0.0.0, PORT=<public port, e.g. 7860>, FLASK_DEBUG=0 to serve publicly.
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT") or os.environ.get("MURMUR_PORT") or "5050")
    debug = os.environ.get("FLASK_DEBUG", "1") not in ("0", "false", "no", "")
    app.run(host=host, port=port, debug=debug)
