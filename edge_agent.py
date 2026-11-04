"""MQTT/HTTP edge gateway with durable batches and bounded retries (ADR-0005)."""

import argparse
import json
import math
import os
import sqlite3
import time
import uuid
from datetime import datetime, timezone
import urllib.request
import urllib.error
from source_profiles import (
    SourceRegistry,
    SourceInputError,
    IncorporationPipeline,
    RegistryReloader,
)
from http_input import make_http_server


class EdgeBuffer:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS sequence_counters(device TEXT PRIMARY KEY, value INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS readings(id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL, recorded_at TEXT NOT NULL, batch_id TEXT);
        CREATE TABLE IF NOT EXISTS batches(id TEXT PRIMARY KEY, payload TEXT NOT NULL);
        """)

    def append(
        self, device, temperature, humidity=None, recorded_at=None, source_context=None
    ):
        with self.db:
            self.db.execute(
                "INSERT INTO sequence_counters(device,value) VALUES (?,0) ON CONFLICT(device) DO NOTHING",
                (device,),
            )
            self.db.execute(
                "UPDATE sequence_counters SET value=value+1 WHERE device=?", (device,)
            )
            sequence = self.db.execute(
                "SELECT value FROM sequence_counters WHERE device=?", (device,)
            ).fetchone()[0]
            at = recorded_at or datetime.now(timezone.utc).isoformat(
                timespec="microseconds"
            )
            payload = {
                "deviceUuid": device,
                "sequenceNumber": sequence,
                "recordedAt": at,
                "temperature": temperature,
                "humidity": humidity,
            }
            if source_context:
                payload.update(source_context)
            self.db.execute(
                "INSERT INTO readings(payload,recorded_at) VALUES (?,?)",
                (json.dumps(payload), at),
            )
        return payload

    def next_batch(self, gateway, limit=500):
        old = self.db.execute(
            "SELECT id,payload FROM batches ORDER BY rowid LIMIT 1"
        ).fetchone()
        if old:
            return old[0], json.loads(old[1])
        rows = self.db.execute(
            "SELECT id,payload FROM readings WHERE batch_id IS NULL ORDER BY recorded_at,id LIMIT ?",
            (min(limit, 500),),
        ).fetchall()
        if not rows:
            return None
        key = str(uuid.uuid4())
        body = {"gatewayUuid": gateway, "readings": [json.loads(r[1]) for r in rows]}
        with self.db:
            self.db.execute(
                "INSERT INTO batches(id,payload) VALUES (?,?)", (key, json.dumps(body))
            )
            self.db.executemany(
                "UPDATE readings SET batch_id=? WHERE id=?", [(key, r[0]) for r in rows]
            )
        return key, body

    def confirm(self, key, result):
        row = self.db.execute(
            "SELECT payload FROM batches WHERE id=?", (key,)
        ).fetchone()
        if not row:
            raise ValueError("Unknown batch")
        expected = len(json.loads(row[0])["readings"])
        accepted = result.get("accepted")
        duplicated = result.get("duplicated")
        if (
            not isinstance(accepted, int)
            or not isinstance(duplicated, int)
            or accepted < 0
            or duplicated < 0
            or accepted + duplicated != expected
        ):
            raise ValueError("Incomplete confirmation; retain batch")
        with self.db:
            self.db.execute("DELETE FROM readings WHERE batch_id=?", (key,))
            self.db.execute("DELETE FROM batches WHERE id=?", (key,))

    def pending(self):
        return self.db.execute("SELECT count(*) FROM readings").fetchone()[0]

    def retention_exceeded(self):
        row = self.db.execute("SELECT min(recorded_at) FROM readings").fetchone()
        return bool(
            row[0]
            and (
                datetime.now(timezone.utc) - datetime.fromisoformat(row[0])
            ).total_seconds()
            > 86400
        )

    def close(self):
        self.db.close()


def post(base, path, body, gateway_key, key=None):
    headers = {
        "Content-Type": "application/json",
        "X-Gateway-Key": gateway_key,
        "X-Correlation-Id": str(uuid.uuid4()),
    }
    if key:
        headers["Idempotency-Key"] = key
    req = urllib.request.Request(
        base.rstrip("/") + path,
        data=json.dumps(body).encode(),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        data = response.read()
        return response.status, json.loads(data) if data else None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="edge.sqlite")
    p.add_argument("--sensors", type=int, default=1)
    p.add_argument("--interval", type=float, default=10)
    p.add_argument("--cut-seconds", type=float, default=0)
    p.add_argument("--duration", type=float, default=60)
    p.add_argument("--mqtt-host")
    p.add_argument("--mqtt-port", type=int, default=1883)
    p.add_argument("--source-profiles", help="Versioned JSON source registry")
    p.add_argument(
        "--source-refresh-seconds",
        type=float,
        default=5,
        help="Interval for validated source file reload (default: 5 seconds)",
    )
    p.add_argument("--http-host", default="127.0.0.1")
    p.add_argument(
        "--http-port", type=int, help="Enable authenticated HTTP source input"
    )
    args = p.parse_args()
    if (
        not math.isfinite(args.source_refresh_seconds)
        or args.source_refresh_seconds <= 0
    ):
        p.error("--source-refresh-seconds must be a finite positive number")
    gateway = os.environ["GATEWAY_UUID"]
    gateway_key = os.environ["GATEWAY_API_KEY"]
    base = os.environ["API_BASE_URL"]
    devices = (
        []
        if args.source_profiles
        else [
            str(uuid.UUID(int=uuid.UUID(os.environ["DEVICE_UUID"]).int + i))
            for i in range(args.sensors)
        ]
    )
    buffer = EdgeBuffer(args.db)
    pipeline = (
        IncorporationPipeline(SourceRegistry.load(args.source_profiles), buffer)
        if args.source_profiles
        else None
    )
    reloader = RegistryReloader(args.source_profiles, pipeline) if pipeline else None
    if args.http_port is not None and pipeline is None:
        p.error("--http-port requires --source-profiles")
    if pipeline and not args.mqtt_host and args.http_port is None:
        p.error("Configure --mqtt-host or --http-port for source profiles")
    http_server = (
        make_http_server(
            (args.http_host, args.http_port),
            pipeline,
            os.environ.get("EDGE_INGEST_TOKEN"),
        )
        if args.http_port is not None
        else None
    )
    started = time.monotonic()
    retry = 1
    next_send = 0
    next_heartbeat = 0
    next_sample = 0
    next_refresh = 0
    client = None
    generated = accepted = duplicated = peak_pending = 0
    recovery_seconds = None
    if args.mqtt_host:
        import paho.mqtt.client as mqtt

        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

        def receive(client, userdata, message):
            nonlocal generated
            try:
                if pipeline:
                    pipeline.receive_mqtt(message.topic, message.payload)
                else:
                    reading = json.loads(message.payload)
                    buffer.append(
                        message.topic.split("/")[1],
                        reading["temperature"],
                        reading.get("humidity"),
                        reading.get("recordedAt"),
                    )
                generated += 1
            except (ValueError, KeyError, TypeError, UnicodeError, SourceInputError):
                print("Rejected incompatible MQTT reading", flush=True)

        client.on_message = receive
        client.connect(args.mqtt_host, args.mqtt_port)
        client.subscribe("coldtrace/+/readings")
    while time.monotonic() - started < args.duration:
        now = time.monotonic()
        if reloader and now >= next_refresh:
            try:
                if reloader.reload_if_changed():
                    print("Source configuration reloaded", flush=True)
            except (OSError, ValueError, UnicodeError):
                print(
                    "Source configuration reload rejected; previous configuration retained",
                    flush=True,
                )
            next_refresh = now + args.source_refresh_seconds
        if client:
            client.loop(timeout=0.05)
        elif not pipeline and now >= next_sample:
            for device in devices:
                buffer.append(device, 4.0, 60.0)
            generated += len(devices)
            next_sample = now + args.interval
        if http_server:
            pending_before_http = buffer.pending()
            http_server.handle_request()
            generated += buffer.pending() - pending_before_http
        peak_pending = max(peak_pending, buffer.pending())
        if now - started >= args.cut_seconds:
            if now >= next_heartbeat:
                try:
                    post(
                        base,
                        "/api/v1/telemetry/heartbeats",
                        {
                            "gatewayUuid": gateway,
                            "sentAt": datetime.now(timezone.utc).isoformat(),
                            "pendingReadings": buffer.pending(),
                        },
                        gateway_key,
                    )
                except (OSError, urllib.error.URLError):
                    pass
                next_heartbeat = now + 10
            if now >= next_send:
                batch = buffer.next_batch(gateway)
                if batch:
                    try:
                        status, result = post(
                            base,
                            "/api/v1/telemetry/batches",
                            batch[1],
                            gateway_key,
                            batch[0],
                        )
                        if status != 202:
                            raise ValueError("Unexpected status")
                        buffer.confirm(batch[0], result)
                        accepted += result["accepted"]
                        duplicated += result["duplicated"]
                        if recovery_seconds is None and buffer.pending() == 0:
                            recovery_seconds = round(
                                time.monotonic() - started - args.cut_seconds, 3
                            )
                        retry = 1
                        next_send = now
                    except (OSError, urllib.error.URLError, ValueError):
                        next_send = now + retry
                        retry = min(60, retry * 2)
        if buffer.retention_exceeded():
            print("Retention exceeded 24h; unconfirmed readings retained", flush=True)
        time.sleep(0.05)
    if client:
        client.disconnect()
    if http_server:
        http_server.server_close()
    print(
        json.dumps(
            {
                "pending": buffer.pending(),
                "generated": generated,
                "accepted": accepted,
                "duplicated": duplicated,
                "peakPending": peak_pending,
                "withheldNetworkSeconds": args.cut_seconds,
                "recoverySeconds": recovery_seconds,
                "elapsedSeconds": round(time.monotonic() - started, 3),
            }
        )
    )
    buffer.close()


if __name__ == "__main__":
    main()
