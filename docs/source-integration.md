# Source integration

The edge accepts explicitly configured MQTT/JSON and HTTP/JSON sources. Source
profiles map field names and units to the existing telemetry contract before a
reading enters SQLite. The same buffer and sender retain sequence numbers,
stable batch payloads and confirmation-driven deletion from the first increment.

## Configuration

Copy `config/source-profiles.example.json` outside the repository and adapt its
device UUIDs to devices provisioned on your gateway. One active source maps to
one device. Configuration schema version 1 supports profile versions greater
than zero, Celsius or Fahrenheit, relative humidity in percent, and ISO 8601
timestamps with an explicit offset. Every adapter resolves its source from this
registry; payloads cannot replace the configured device identity.

The registry is read at startup. Replacing a source or changing a profile requires
updating the configuration and restarting the agent. The cloud registry API,
tenant authorization for configuration, historical associations and a management
screen are separate work items for US03, US29 and US30; this increment implements
the edge contract and adapters supporting those stories.

## Run

Set `GATEWAY_UUID`, `GATEWAY_API_KEY`, and `API_BASE_URL` as in the existing agent.
Set `EDGE_INGEST_TOKEN` to an independent random value of at least 24 characters.
The input token is different from the key used to send batches to Monitoring.

```sh
python edge_agent.py --db /data/edge.sqlite \
  --source-profiles /config/source-profiles.json \
  --mqtt-host localhost --http-port 8091 --duration 3600
```

MQTT messages use `coldtrace/{sourceId}/readings`. With the example profile:

```json
{"temperature":4.0,"humidity":60,"recordedAt":"2026-11-03T14:00:00-05:00"}
```

HTTP accepts `POST /api/v1/sources/{sourceId}/readings` with `Content-Type:
application/json` and `Authorization: Bearer <EDGE_INGEST_TOKEN>`. The example
`camara-verduras` profile accepts:

```json
{"tempF":39.2,"rh":60,"timestamp":"2026-11-03T19:00:00Z"}
```

Both represent 4 °C and 60 %RH at the same UTC instant. The result is stored with
`sourceId`, `profileVersion`, `schemaVersion: 1`, `temperatureUnit: C` and
`humidityUnit: %RH`; MQTT and HTTP sources retain their configured device UUIDs.
These provenance fields survive edge restarts and batch retries. Current
Monitoring accepts the existing measurement fields; durable cloud-side provenance
and explicit canonical contract validation are subsequent integration work.

The HTTP adapter binds to `127.0.0.1` by default and runs on the same event loop
as MQTT and SQLite. Use it on the local edge or behind a TLS terminator on a
trusted network. It is not an internet-facing web server. When mapping a Docker
port, bind the published port to loopback and set `--http-host 0.0.0.0` inside the
container. Mount `/data` persistently and mount the profile file read-only.

## Responses

| HTTP | Meaning |
|---|---|
| 202 | Reading stored at the edge; response includes its sequence number. Cloud delivery is confirmed separately. |
| 400 | Invalid JSON, unknown source, unsupported version/unit or invalid measurement/time. No reading is stored. |
| 401 | Missing or invalid edge input credential. |
| 404 | Unknown route. |
| 408 | Body read timed out. |
| 413 | Body size is outside 1–65536 bytes. |
| 415 | Content type is not JSON. |

## Verification

```sh
python -m unittest discover -v
docker build -t coldtrace-edge-gateway:source-profiles .
```

The suite covers 100 measurements per profile, equivalent units and instants,
identity mapping, invalid input without persistence or sequence advancement,
authenticated HTTP loopback requests, and provenance/retry stability after a
SQLite restart. The original six store-and-forward regression tests also run.

The example profiles and tests use simulated devices. Network MQTT broker and
physical sensor trials are distinct from these adapter/contract tests.
