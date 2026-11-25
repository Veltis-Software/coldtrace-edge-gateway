"""Versioned source mappings shared by MQTT and HTTP input adapters."""

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import hashlib
import math
import re
import uuid


class SourceInputError(ValueError):
    """A source reading cannot be interpreted using its configured profile."""


def _configuration_bytes(path):
    with open(path, "rb") as source:
        content = source.read(262145)
    if len(content) > 262144:
        raise SourceInputError("Source configuration exceeds 256 KiB")
    return content


@dataclass(frozen=True)
class SourceProfile:
    source_id: str
    device_uuid: str
    transport: str
    version: int
    temperature_field: str
    humidity_field: str
    time_field: str
    temperature_unit: str


class SourceRegistry:
    def __init__(self, document):
        if (
            not isinstance(document, dict)
            or type(document.get("schemaVersion")) is not int
            or document.get("schemaVersion") != 1
        ):
            raise SourceInputError("Unsupported source configuration version")
        rows = document.get("sources")
        if not isinstance(rows, list) or not rows:
            raise SourceInputError("At least one configured source is required")
        self._profiles = {}
        devices = set()
        for row in rows:
            try:
                source_id = row["sourceId"]
                device = str(uuid.UUID(row["deviceUuid"]))
                transport = row["transport"]
                version = row["profileVersion"]
                fields = row["fields"]
                unit = row["temperatureUnit"]
                if (
                    not isinstance(source_id, str)
                    or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", source_id)
                    or source_id in self._profiles
                    or device in devices
                    or transport not in ("mqtt", "http")
                    or type(version) is not int
                    or version < 1
                    or unit not in ("C", "F")
                    or row.get("humidityUnit", "%RH") != "%RH"
                    or any(
                        not isinstance(fields[k], str) or not fields[k]
                        for k in ("temperature", "humidity", "recordedAt")
                    )
                    or set(fields) != {"temperature", "humidity", "recordedAt"}
                    or len(set(fields.values())) != 3
                ):
                    raise SourceInputError("Invalid or duplicated source profile")
                self._profiles[source_id] = SourceProfile(
                    source_id,
                    device,
                    transport,
                    version,
                    fields["temperature"],
                    fields["humidity"],
                    fields["recordedAt"],
                    unit,
                )
                devices.add(device)
            except (KeyError, TypeError, ValueError, AttributeError) as exc:
                raise SourceInputError("Invalid source configuration") from exc

    @classmethod
    def load(cls, path):
        return cls(json.loads(_configuration_bytes(path).decode("utf-8-sig")))

    def resolve(self, source_id, transport):
        profile = self._profiles.get(source_id)
        if profile is None or profile.transport != transport:
            raise SourceInputError("Source is not configured for this transport")
        return profile


def _number(value, field):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SourceInputError(f"Invalid {field}")
    try:
        finite = math.isfinite(value)
    except OverflowError:
        finite = False
    if not finite:
        raise SourceInputError(f"Invalid {field}")
    return value


def normalize(profile, payload):
    """Normalize before assigning a sequence or writing anything to SQLite."""
    if not isinstance(payload, dict):
        raise SourceInputError("A JSON object is required")
    if (
        type(payload.get("schemaVersion", 1)) is not int
        or payload.get("schemaVersion", 1) != 1
    ):
        raise SourceInputError("Unsupported reading schema version")
    if (
        payload.get("temperatureUnit", profile.temperature_unit)
        != profile.temperature_unit
    ):
        raise SourceInputError("Temperature unit does not match the configured profile")
    if payload.get("humidityUnit", "%RH") != "%RH":
        raise SourceInputError("Unsupported humidity unit")
    if (
        type(payload.get("profileVersion", profile.version)) is not int
        or payload.get("profileVersion", profile.version) != profile.version
    ):
        raise SourceInputError("Reading profile version does not match configuration")
    try:
        temperature = _number(payload[profile.temperature_field], "temperature")
        humidity = payload.get(profile.humidity_field)
        if humidity is not None:
            humidity = _number(humidity, "humidity")
            if not 0 <= humidity <= 100:
                raise SourceInputError("Humidity must be between 0 and 100 %RH")
        at = datetime.fromisoformat(payload[profile.time_field].replace("Z", "+00:00"))
        if at.tzinfo is None or at.utcoffset() is None:
            raise SourceInputError("A timestamp with an explicit offset is required")
        recorded_at = at.astimezone(timezone.utc).isoformat(timespec="microseconds")
    except (KeyError, TypeError, AttributeError, ValueError, OverflowError) as exc:
        raise SourceInputError("Invalid measurement or timestamp") from exc
    if profile.temperature_unit == "F":
        temperature = (temperature - 32) / 9 * 5
    return {
        "device": profile.device_uuid,
        "temperature": temperature,
        "humidity": humidity,
        "recorded_at": recorded_at,
        "source_context": {
            "sourceId": profile.source_id,
            "profileVersion": profile.version,
            "schemaVersion": 1,
            "temperatureUnit": "C",
            "humidityUnit": "%RH",
        },
    }


class IncorporationPipeline:
    def __init__(self, registry, buffer):
        self.registry = registry
        self.buffer = buffer

    def ingest(self, source_id, payload, transport):
        profile = self.registry.resolve(source_id, transport)
        return self.buffer.append(**normalize(profile, payload))

    def receive_mqtt(self, topic, payload):
        parts = topic.split("/")
        if len(parts) != 3 or parts[0] != "coldtrace" or parts[2] != "readings":
            raise SourceInputError("Unsupported MQTT topic")
        try:
            reading = json.loads(payload)
        except (ValueError, UnicodeError) as exc:
            raise SourceInputError("Malformed JSON") from exc
        return self.ingest(parts[1], reading, "mqtt")


class RegistryReloader:
    """Validate the whole replacement before swapping the live registry on the agent loop."""

    def __init__(self, path, pipeline):
        self.path = path
        self.pipeline = pipeline
        self._fingerprint = None
        self.reload_if_changed()

    def reload_if_changed(self):
        content = _configuration_bytes(self.path)
        fingerprint = hashlib.sha256(content).digest()
        if fingerprint == self._fingerprint:
            return False
        registry = SourceRegistry(json.loads(content.decode("utf-8-sig")))
        self.pipeline.registry = registry
        self._fingerprint = fingerprint
        return True
