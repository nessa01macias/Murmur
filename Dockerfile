# Murmur fleet — long-running worker that runs coordination rounds through the Aiven MCP.
# No direct DB/Kafka drivers: it's an MCP client (mcp) + claude-sonnet (anthropic) + local
# embeddings (fastembed). Secrets are injected at deploy time, never baked into the image.
FROM python:3.11-slim

WORKDIR /app

# Python deps.
RUN pip install --no-cache-dir "mcp>=1.0" "anthropic>=0.40" "fastembed>=0.4"

# Bake the embedding model into the image so the worker never downloads it at runtime.
RUN python -c "from fastembed import TextEmbedding; TextEmbedding('BAAI/bge-small-en-v1.5')"

COPY murmur.py .

# Aiven Apps requires a listening port; the worker serves a status endpoint here and runs
# coordination rounds in the background.
ENV PORT=8080 \
    MURMUR_SERVE=1 \
    MURMUR_FLEET=6 \
    MURMUR_INTERVAL=300
EXPOSE 8080

# ANTHROPIC_API_KEY and AIVEN_TOKEN are injected as secrets at deploy time — never baked in.
CMD ["python", "murmur.py", "--serve"]
