# ColdTrace Edge Gateway

Python 3.12 agent with SQLite WAL persistence. It retains unconfirmed readings,
persistent device sequences and stable retry batch identities/payloads across
restarts. Batches are chronological and contain at most 500 readings. Readings
are removed only after complete accepted/duplicated confirmation.

## Configuration and execution

Set `API_BASE_URL`, `GATEWAY_UUID`, `DEVICE_UUID` and `GATEWAY_API_KEY`.
`DEVICE_UUID` starts the consecutive UUID sequence for simulated sensors.

```sh
python -m unittest discover -v
python edge_agent.py --db edge.sqlite --sensors 50 --interval 10 --duration 330 --cut-seconds 300
docker build -t coldtrace-edge-gateway:dev .
```

The image runs without root; mount a writable persistent volume at `/data`.
Heartbeat interval is 10 seconds; retry backoff is 1–60 seconds. Readings older
than 24 hours trigger a warning and are retained until confirmed. The simulator
prints generated/accepted/duplicate/pending counts and measured recovery time.

## MQTT adapter

Install `requirements.txt` and pass `--mqtt-host` to subscribe to
`coldtrace/+/readings` with paho-mqtt 2.1. Messages contain temperature, optional
humidity and recordedAt; the topic identifies the device UUID. The real MQTT
adapter still requires field/integration validation. Automated tests cover
durability, sequence persistence, stable retries, confirmation rules and limits.
