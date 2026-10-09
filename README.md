# ColdTrace Edge Gateway — Sprint 1

Python 3.12; SQLite WAL retains unconfirmed readings and stable batch identities
across restarts. `python -m unittest discover -v` runs the buffer tests.
Set API_BASE_URL, GATEWAY_UUID, DEVICE_UUID and GATEWAY_API_KEY explicitly.
`python edge_agent.py --help` describes the simulator and network-cut options.
MQTT uses optional paho-mqtt 2.1; the real MQTT adapter has not been field tested.
The Docker image runs as a non-root user. Mount a writable volume for SQLite.
