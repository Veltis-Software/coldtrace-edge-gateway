FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt && useradd --system --uid 10001 coldtrace && mkdir /data && chown 10001:10001 /data
COPY edge_agent.py ./
USER 10001
ENTRYPOINT ["python", "edge_agent.py", "--db", "/data/edge.sqlite"]
