# Murmur — ONE container that runs the agent swarm (conductor) + the live dashboard.
# Deploys anywhere that builds a Dockerfile; tuned for Hugging Face Spaces (Docker SDK, port 7860).
# The public URL is the live wall; the agents run inside, talking to Aiven entirely via the MCP.
# Secrets (ANTHROPIC_API_KEY, AIVEN_TOKEN, DATABASE_URL) are injected at runtime — never baked in.
FROM python:3.11-slim

WORKDIR /app
RUN pip install --no-cache-dir "mcp>=1.0" "anthropic>=0.40" "fastembed>=0.4" \
    "flask>=3.0" "psycopg[binary]>=3.1" "python-dotenv>=1.0"

COPY murmur.py .
COPY dashboard/ dashboard/

# HF Spaces serves the port declared as app_port (7860). The dashboard binds it publicly;
# the conductor keeps its own status endpoint on 8080 (internal).
ENV HOST=0.0.0.0 FLASK_DEBUG=0 MURMUR_FLEET=6 MURMUR_INTERVAL=180 \
    HOME=/tmp PYTHONUNBUFFERED=1
EXPOSE 7860

# agents (conductor) in the background + the public dashboard in the foreground
CMD ["bash","-lc","PORT=8080 python murmur.py --serve & exec env PORT=7860 python dashboard/app.py"]
