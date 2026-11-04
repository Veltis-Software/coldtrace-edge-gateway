from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import http.client
import json
from pathlib import Path
import tempfile
import unittest

from edge_agent import EdgeBuffer
from http_input import make_http_server
from source_profiles import (
    SourceRegistry,
    SourceInputError,
    IncorporationPipeline,
    RegistryReloader,
)

CONFIG = Path(__file__).parent / "config" / "source-profiles.example.json"
TOKEN = "local-integration-token-not-a-production-secret"


class SourceIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "readings.sqlite"
        self.buffer = EdgeBuffer(self.path)
        self.config = json.loads(CONFIG.read_text())
        self.registry = SourceRegistry(self.config)
        self.pipeline = IncorporationPipeline(self.registry, self.buffer)

    def tearDown(self):
        self.buffer.close()
        self.tmp.cleanup()

    def mqtt(self, **overrides):
        return {
            "temperature": 4.0,
            "humidity": 60,
            "recordedAt": "2026-11-03T14:00:00-05:00",
            **overrides,
        }

    def http(self, **overrides):
        return {
            "tempF": 39.2,
            "rh": 60,
            "timestamp": "2026-11-03T19:00:00Z",
            **overrides,
        }

    def test_two_profiles_normalize_one_hundred_measurements_each(self):
        for i in range(100):
            celsius = -20 + i / 2
            a = self.pipeline.receive_mqtt(
                "coldtrace/camara-lacteos/readings",
                json.dumps(self.mqtt(temperature=celsius)),
            )
            b = self.pipeline.ingest(
                "camara-verduras", self.http(tempF=celsius * 9 / 5 + 32), "http"
            )
            self.assertAlmostEqual(a["temperature"], b["temperature"], places=8)
            self.assertEqual(a["recordedAt"], b["recordedAt"])
            self.assertEqual(i + 1, a["sequenceNumber"])
            self.assertEqual("camara-lacteos", a["sourceId"])
            self.assertEqual("camara-verduras", b["sourceId"])
            self.assertEqual("C", b["temperatureUnit"])
            self.assertEqual("%RH", b["humidityUnit"])
            self.assertEqual(1, b["schemaVersion"])
        self.assertEqual(200, self.buffer.pending())

    def test_source_reload_rejects_retired_input_without_rewriting_durable_batches(
        self,
    ):
        path = Path(self.tmp.name) / "profiles.json"
        path.write_text(json.dumps(self.config), encoding="utf-8")
        reloader = RegistryReloader(path, self.pipeline)
        old = self.pipeline.receive_mqtt(
            "coldtrace/camara-lacteos/readings", json.dumps(self.mqtt())
        )
        batch = self.buffer.next_batch("11111111-1111-1111-1111-111111111111")
        replacement = deepcopy(self.config)
        replacement["sources"][0].update(
            sourceId="camara-lacteos-repuesto", transport="http", temperatureUnit="F"
        )
        path.write_text(json.dumps(replacement), encoding="utf-8")
        self.assertTrue(reloader.reload_if_changed())
        self.assertFalse(reloader.reload_if_changed())
        with self.assertRaises(SourceInputError):
            self.pipeline.receive_mqtt(
                "coldtrace/camara-lacteos/readings", json.dumps(self.mqtt())
            )
        new = self.pipeline.ingest(
            "camara-lacteos-repuesto", self.mqtt(temperature=39.2), "http"
        )
        self.assertEqual(old["deviceUuid"], new["deviceUuid"])
        self.assertEqual(old["sequenceNumber"] + 1, new["sequenceNumber"])
        self.assertAlmostEqual(4, new["temperature"])
        self.assertEqual(
            batch, self.buffer.next_batch("11111111-1111-1111-1111-111111111111")
        )
        self.assertEqual(2, self.buffer.pending())

    def test_invalid_or_missing_reload_keeps_the_last_complete_registry(self):
        path = Path(self.tmp.name) / "profiles.json"
        path.write_text(json.dumps(self.config), encoding="utf-8")
        reloader = RegistryReloader(path, self.pipeline)
        previous = self.pipeline.registry
        for content in (
            "{",
            json.dumps({"schemaVersion": 2, "sources": []}),
            "x" * 262145,
        ):
            path.write_text(content, encoding="utf-8")
            with self.assertRaises(ValueError):
                reloader.reload_if_changed()
            self.assertIs(previous, self.pipeline.registry)
        path.unlink()
        with self.assertRaises(OSError):
            reloader.reload_if_changed()
        self.assertIs(previous, self.pipeline.registry)
        self.pipeline.receive_mqtt(
            "coldtrace/camara-lacteos/readings", json.dumps(self.mqtt())
        )
        self.assertEqual(1, self.buffer.pending())

    def test_rejected_readings_do_not_write_or_advance_sequence(self):
        cases = [
            self.mqtt(temperatureUnit="K"),
            self.mqtt(humidityUnit="fraction"),
            self.mqtt(schemaVersion=2),
            self.mqtt(schemaVersion=True),
            self.mqtt(profileVersion=2),
            self.mqtt(profileVersion=True),
            self.mqtt(temperature=float("nan")),
            self.mqtt(temperature=True),
            self.mqtt(temperature=float("inf")),
            self.mqtt(temperature="4"),
            self.mqtt(humidity=-1),
            self.mqtt(humidity=101),
            self.mqtt(recordedAt="2026-11-03T19:00:00"),
            self.mqtt(recordedAt="invalid"),
            {},
            [],
            None,
        ]
        for payload in cases:
            with self.subTest(payload=payload):
                with self.assertRaises(SourceInputError):
                    self.pipeline.ingest("camara-lacteos", payload, "mqtt")
                self.assertEqual(0, self.buffer.pending())
        self.assertEqual(
            1,
            self.pipeline.ingest("camara-lacteos", self.mqtt(), "mqtt")[
                "sequenceNumber"
            ],
        )

    def test_unknown_sources_and_wrong_transports_are_rejected(self):
        for source, transport in [
            ("unknown", "mqtt"),
            ("camara-lacteos", "http"),
            ("camara-verduras", "mqtt"),
        ]:
            with self.subTest(source=source, transport=transport), self.assertRaises(
                SourceInputError
            ):
                self.pipeline.ingest(source, self.mqtt(), transport)
        self.assertEqual(0, self.buffer.pending())

    def test_invalid_topics_and_json_are_rejected(self):
        for topic, data in [
            ("coldtrace/a/readings/extra", b"{}"),
            ("other/a/readings", b"{}"),
            ("coldtrace/camara-lacteos/readings", b"{"),
            ("coldtrace/camara-lacteos/readings", b"\xff"),
        ]:
            with self.subTest(topic=topic), self.assertRaises(SourceInputError):
                self.pipeline.receive_mqtt(topic, data)
        self.assertEqual(0, self.buffer.pending())

    def test_provenance_and_retry_payload_survive_restart(self):
        self.pipeline.ingest("camara-verduras", self.http(), "http")
        batch = self.buffer.next_batch("gateway")
        self.buffer.close()
        self.buffer = EdgeBuffer(self.path)
        self.assertEqual(batch, self.buffer.next_batch("gateway"))
        self.assertEqual(1, batch[1]["readings"][0]["profileVersion"])
        self.buffer.confirm(batch[0], {"accepted": 0, "duplicated": 1})
        self.assertEqual(0, self.buffer.pending())

    def test_device_identity_comes_from_configuration(self):
        result = self.pipeline.ingest(
            "camara-verduras",
            self.http(deviceUuid="untrusted-device", sourceId="untrusted-source"),
            "http",
        )
        self.assertEqual(self.config["sources"][1]["deviceUuid"], result["deviceUuid"])
        self.assertEqual("camara-verduras", result["sourceId"])

    def test_duplicate_sources_devices_and_unsupported_configuration_are_rejected(self):
        for key, value in [
            ("sourceId", "camara-lacteos"),
            ("deviceUuid", self.config["sources"][0]["deviceUuid"]),
            ("transport", "unknown"),
            ("profileVersion", True),
            ("temperatureUnit", "K"),
            ("humidityUnit", "fraction"),
        ]:
            doc = deepcopy(self.config)
            doc["sources"][1][key] = value
            with self.subTest(key=key), self.assertRaises(SourceInputError):
                SourceRegistry(doc)
        for version in [2, True, "1"]:
            doc = deepcopy(self.config)
            doc["schemaVersion"] = version
            with self.subTest(version=version), self.assertRaises(SourceInputError):
                SourceRegistry(doc)

    def request(
        self,
        body,
        token=TOKEN,
        path="/api/v1/sources/camara-verduras/readings",
        content_type="application/json",
    ):
        server = make_http_server(("127.0.0.1", 0), self.pipeline, TOKEN)

        def send():
            conn = http.client.HTTPConnection(*server.server_address, timeout=3)
            try:
                conn.request(
                    "POST",
                    path,
                    body,
                    {"Authorization": "Bearer " + token, "Content-Type": content_type},
                )
                response = conn.getresponse()
                return response.status, json.loads(response.read())
            finally:
                conn.close()

        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(send)
                while not future.done():
                    server.handle_request()
                return future.result()
        finally:
            server.server_close()

    def test_http_adapter_persists_canonical_reading(self):
        status, result = self.request(json.dumps(self.http()))
        self.assertEqual(202, status)
        self.assertEqual({"accepted": 1, "sequenceNumber": 1}, result)
        self.assertEqual(1, self.buffer.pending())
        self.assertAlmostEqual(
            4.0, self.buffer.next_batch("gateway")[1]["readings"][0]["temperature"]
        )

    def test_http_rejections_never_persist(self):
        cases = [
            (
                json.dumps(self.http()),
                "wrong-token",
                "/api/v1/sources/camara-verduras/readings",
                "application/json",
                401,
            ),
            (
                "{",
                TOKEN,
                "/api/v1/sources/camara-verduras/readings",
                "application/json",
                400,
            ),
            (
                json.dumps(self.http(temperatureUnit="K")),
                TOKEN,
                "/api/v1/sources/camara-verduras/readings",
                "application/json",
                400,
            ),
            (
                json.dumps(self.http()),
                TOKEN,
                "/api/v1/sources/missing/readings",
                "application/json",
                400,
            ),
            (json.dumps(self.http()), TOKEN, "/wrong-route", "application/json", 404),
            (
                json.dumps(self.http()),
                TOKEN,
                "/api/v1/sources/camara-verduras/readings",
                "text/plain",
                415,
            ),
            (
                "x" * 65537,
                TOKEN,
                "/api/v1/sources/camara-verduras/readings",
                "application/json",
                413,
            ),
        ]
        for body, token, path, content_type, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(
                    expected, self.request(body, token, path, content_type)[0]
                )
                self.assertEqual(0, self.buffer.pending())

    def test_http_requires_a_separate_strong_token(self):
        for token in [None, "", "short"]:
            with self.assertRaises(ValueError):
                make_http_server(("127.0.0.1", 0), self.pipeline, token)


if __name__ == "__main__":
    unittest.main()
