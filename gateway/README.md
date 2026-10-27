# MQTT-SN to MQTT Gateway

This Python gateway receives the subset of MQTT-SN v1.2 used by the bundled publisher over UDP and forwards its messages to an MQTT broker using MQTT/TCP.

Supported MQTT-SN operations are `CONNECT`, `REGISTER`, QoS 0 `PUBLISH` using normal topic IDs, `PINGREQ`, and `DISCONNECT`. This is a small lab gateway, not a complete implementation of every MQTT-SN feature.

## Configuration

Settings are loaded from `config.yaml` by default. Override the path with `CONFIG_PATH`.

- `mqtt_sn.bind_host` and `mqtt_sn.bind_port`: UDP address for MQTT-SN clients.
- `mqtt_sn.session_timeout_seconds`: idle session cleanup interval.
- `mqtt.host` and `mqtt.port`: regular MQTT broker address.
- `mqtt.client_id` and `mqtt.keep_alive_seconds`: gateway's MQTT connection settings.

## Run standalone

```bash
pip install -r requirements.txt
python mqttsn_gateway.py
```

The broker needs to be reachable at the address in `config.yaml`.
