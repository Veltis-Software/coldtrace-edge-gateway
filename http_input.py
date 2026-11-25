"""Bounded authenticated HTTP input; served on the edge event loop."""

import hmac
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import re

from source_profiles import SourceInputError

MAX_BODY_BYTES = 65536


def make_http_server(address, pipeline, token):
    if not token or len(token) < 24:
        raise ValueError("EDGE_INGEST_TOKEN must contain at least 24 characters")

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.0"

        def setup(self):
            super().setup()
            self.connection.settimeout(2)

        def log_message(self, *args):
            pass  # Do not log payloads or authentication headers.

        def respond(self, status, body):
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_POST(self):
            if not hmac.compare_digest(
                self.headers.get("Authorization", "").encode(),
                ("Bearer " + token).encode(),
            ):
                self.respond(401, {"error": "Invalid edge input credential"})
                return
            match = re.fullmatch(
                r"/api/v1/sources/([A-Za-z0-9_-]{1,64})/readings", self.path
            )
            if not match:
                self.respond(404, {"error": "Unknown source input route"})
                return
            if self.headers.get_content_type() != "application/json":
                self.respond(415, {"error": "Content-Type must be application/json"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0 or length > MAX_BODY_BYTES:
                    # Drain only a bounded amount so a slightly oversized request
                    # can receive the rejection instead of a TCP reset on close.
                    if length > MAX_BODY_BYTES:
                        self.rfile.read(min(length, MAX_BODY_BYTES + 1))
                    self.respond(413, {"error": "Body must contain 1 to 65536 bytes"})
                    return
                raw = self.rfile.read(length)
                if len(raw) != length:
                    raise ValueError("Truncated body")
                reading = pipeline.ingest(match[1], json.loads(raw), "http")
            except (ValueError, UnicodeError, SourceInputError):
                self.respond(
                    400,
                    {
                        "error": "Reading is incompatible with the configured source profile"
                    },
                )
                return
            except TimeoutError:
                self.respond(408, {"error": "Request body timeout"})
                return
            self.respond(
                202, {"accepted": 1, "sequenceNumber": reading["sequenceNumber"]}
            )

    server = HTTPServer(address, Handler)
    server.timeout = 0.01
    return server
