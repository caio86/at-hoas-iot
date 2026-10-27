"""Small MQTT-SN v1.2 UDP gateway that forwards QoS 0 publications to MQTT."""
from __future__ import annotations

import logging
import math
import os
import signal
import socket
import time
from dataclasses import dataclass, field
from pathlib import Path
from threading import Event
from typing import Any

import yaml

LOG = logging.getLogger("mqttsn_gateway")

# MQTT-SN v1.2 packet types used by the bundled publisher.
CONNECT = 0x04
CONNACK = 0x05
REGISTER = 0x0A
REGACK = 0x0B
PUBLISH = 0x0C
PINGREQ = 0x16
PINGRESP = 0x17
DISCONNECT = 0x18

RETURN_ACCEPTED = 0x00
RETURN_INVALID_TOPIC_ID = 0x02
RETURN_NOT_SUPPORTED = 0x03


def encode_packet(message_type: int, body: bytes = b"") -> bytes:
    """Encode an MQTT-SN packet using the short or extended length header."""
    if not 0 <= message_type <= 0xFF:
        raise ValueError("message_type must fit in one byte")

    short_length = len(body) + 2
    if short_length <= 0xFF:
        return bytes((short_length, message_type)) + body

    extended_length = len(body) + 4
    if extended_length > 0xFFFF:
        raise ValueError("MQTT-SN packet exceeds the 65535-byte protocol limit")
    return b"\x01" + extended_length.to_bytes(2, "big") + bytes((message_type,)) + body


def decode_packet(packet: bytes) -> tuple[int, bytes]:
    """Decode and validate an MQTT-SN packet, returning type and body."""
    if len(packet) < 2:
        raise ValueError("MQTT-SN packet is too short")

    if packet[0] == 0x01:
        if len(packet) < 4:
            raise ValueError("MQTT-SN extended header is too short")
        declared_length = int.from_bytes(packet[1:3], "big")
        message_type = packet[3]
        body = packet[4:]
    else:
        declared_length = packet[0]
        message_type = packet[1]
        body = packet[2:]

    if declared_length != len(packet):
        raise ValueError(
            f"MQTT-SN packet length mismatch: header says {declared_length}, "
            f"datagram contains {len(packet)} bytes"
        )
    return message_type, body


@dataclass(frozen=True)
class GatewaySettings:
    bind_host: str
    bind_port: int
    session_timeout_seconds: float
    mqtt_host: str
    mqtt_port: int
    mqtt_client_id: str
    mqtt_keep_alive_seconds: int


@dataclass
class ClientSession:
    client_id: str
    topic_to_id: dict[str, int] = field(default_factory=dict)
    id_to_topic: dict[int, str] = field(default_factory=dict)
    next_topic_id: int = 1
    last_seen: float = field(default_factory=time.monotonic)

    def register_topic(self, topic_name: str) -> int:
        """Return a stable per-session topic ID, allocating it if needed."""
        existing = self.topic_to_id.get(topic_name)
        if existing is not None:
            return existing

        for _ in range(0xFFFF):
            topic_id = self.next_topic_id
            self.next_topic_id = (self.next_topic_id % 0xFFFF) + 1
            if topic_id not in self.id_to_topic:
                self.topic_to_id[topic_name] = topic_id
                self.id_to_topic[topic_id] = topic_name
                return topic_id

        raise RuntimeError("No MQTT-SN topic IDs remain for this client session")


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{location} must be a YAML mapping")
    return value


def _integer(value: Any, location: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{location} must be an integer between {minimum} and {maximum}")
    return value


def load_config(path: str | Path) -> GatewaySettings:
    """Load and validate gateway settings from YAML."""
    try:
        with open(path, "r", encoding="utf-8") as stream:
            raw = yaml.safe_load(stream)
    except FileNotFoundError as exc:
        raise ValueError(f"Config file not found: {path}") from exc
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML in config file {path}: {exc}") from exc

    root = _mapping(raw, "config root")
    mqtt_sn = _mapping(root.get("mqtt_sn"), "mqtt_sn")
    mqtt_settings = _mapping(root.get("mqtt"), "mqtt")

    bind_host = mqtt_sn.get("bind_host", "0.0.0.0")
    mqtt_host = mqtt_settings.get("host")
    mqtt_client_id = mqtt_settings.get("client_id", "mqttsn-gateway")
    for value, location in (
        (bind_host, "mqtt_sn.bind_host"),
        (mqtt_host, "mqtt.host"),
        (mqtt_client_id, "mqtt.client_id"),
    ):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{location} must be a non-empty string")

    session_timeout = mqtt_sn.get("session_timeout_seconds", 300)
    if (
        isinstance(session_timeout, bool)
        or not isinstance(session_timeout, (int, float))
        or not math.isfinite(float(session_timeout))
        or session_timeout <= 0
    ):
        raise ValueError("mqtt_sn.session_timeout_seconds must be a positive finite number")

    return GatewaySettings(
        bind_host=bind_host.strip(),
        bind_port=_integer(mqtt_sn.get("bind_port", 1884), "mqtt_sn.bind_port", 1, 65535),
        session_timeout_seconds=float(session_timeout),
        mqtt_host=mqtt_host.strip(),
        mqtt_port=_integer(mqtt_settings.get("port", 1883), "mqtt.port", 1, 65535),
        mqtt_client_id=mqtt_client_id.strip(),
        mqtt_keep_alive_seconds=_integer(
            mqtt_settings.get("keep_alive_seconds", 60), "mqtt.keep_alive_seconds", 0, 65535
        ),
    )


class MqttBrokerBridge:
    """Own the regular MQTT connection and forward payloads to the broker."""

    def __init__(self, settings: GatewaySettings) -> None:
        # Delay the Paho import so protocol/session unit tests don't need an MQTT
        # client dependency unless they instantiate the real broker bridge.
        import paho.mqtt.client as mqtt_client

        self.settings = settings
        self._mqtt = mqtt_client
        self.connected = Event()
        self.client = mqtt_client.Client(
            callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2,
            client_id=settings.mqtt_client_id,
            protocol=mqtt_client.MQTTv311,
        )
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_connect_fail = self._on_connect_fail
        self.client.reconnect_delay_set(min_delay=1, max_delay=30)

    def start(self) -> None:
        """Start Paho's background loop and reconnecting connection attempt."""
        self.client.connect_async(
            self.settings.mqtt_host,
            self.settings.mqtt_port,
            keepalive=self.settings.mqtt_keep_alive_seconds,
        )
        self.client.loop_start()
        LOG.info(
            "Connecting to MQTT broker %s:%d as %s",
            self.settings.mqtt_host,
            self.settings.mqtt_port,
            self.settings.mqtt_client_id,
        )

    def _on_connect(self, _client: Any, _userdata: Any, _flags: Any, reason_code: Any, _properties: Any) -> None:
        if reason_code == 0:
            self.connected.set()
            LOG.info("Connected to MQTT broker %s:%d", self.settings.mqtt_host, self.settings.mqtt_port)
        else:
            self.connected.clear()
            LOG.error("MQTT broker refused connection: %s", reason_code)

    def _on_disconnect(self, _client: Any, _userdata: Any, _disconnect_flags: Any, reason_code: Any, _properties: Any) -> None:
        self.connected.clear()
        if reason_code != 0:
            LOG.warning("Disconnected from MQTT broker: %s", reason_code)

    def _on_connect_fail(self, _client: Any, _userdata: Any) -> None:
        self.connected.clear()
        LOG.warning(
            "Unable to connect to MQTT broker %s:%d; Paho will retry",
            self.settings.mqtt_host,
            self.settings.mqtt_port,
        )

    def publish(self, topic: str, payload: bytes, retain: bool) -> bool:
        """Queue a QoS 0 message for the MQTT broker, reporting local errors."""
        if not self.connected.is_set():
            LOG.warning("MQTT broker is disconnected; dropping message topic=%s", topic)
            return False

        info = self.client.publish(topic, payload=payload, qos=0, retain=retain)
        if info.rc != self._mqtt.MQTT_ERR_SUCCESS:
            LOG.warning(
                "Could not queue MQTT message: topic=%s rc=%s",
                topic,
                info.rc,
            )
            return False
        return True

    def close(self) -> None:
        """Stop Paho's worker loop and disconnect cleanly."""
        try:
            self.client.disconnect()
        finally:
            self.client.loop_stop()
            self.connected.clear()


class MqttSnGateway:
    """Handle the supported MQTT-SN packets and bridge publications to MQTT."""

    def __init__(self, settings: GatewaySettings, bridge: MqttBrokerBridge) -> None:
        self.settings = settings
        self.bridge = bridge
        self.sessions: dict[tuple[str, int], ClientSession] = {}
        self.socket: socket.socket | Any | None = None

    def _send(self, address: tuple[str, int], message_type: int, body: bytes = b"") -> None:
        if self.socket is None:
            raise RuntimeError("UDP listener socket is not initialized")
        self.socket.sendto(encode_packet(message_type, body), address)

    def handle_datagram(self, packet: bytes, address: tuple[str, int]) -> None:
        """Process one UDP datagram from an MQTT-SN client."""
        message_type, body = decode_packet(packet)
        session = self.sessions.get(address)
        if session is not None:
            session.last_seen = time.monotonic()

        if message_type == CONNECT:
            self._handle_connect(body, address)
        elif message_type == REGISTER:
            self._handle_register(body, address)
        elif message_type == PUBLISH:
            self._handle_publish(body, address)
        elif message_type == PINGREQ:
            if session is not None:
                self._send(address, PINGRESP)
            else:
                LOG.warning("Ignoring PINGREQ from unconnected client %s:%d", *address)
        elif message_type == DISCONNECT:
            removed = self.sessions.pop(address, None)
            if removed:
                LOG.info("MQTT-SN client disconnected: id=%s addr=%s:%d", removed.client_id, *address)
        else:
            LOG.debug("Ignoring unsupported MQTT-SN packet type 0x%02x from %s", message_type, address)

    def _handle_connect(self, body: bytes, address: tuple[str, int]) -> None:
        if len(body) < 4:
            raise ValueError("CONNECT body is too short")

        flags, protocol_id = body[0], body[1]
        keep_alive = int.from_bytes(body[2:4], "big")
        try:
            client_id = body[4:].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("CONNECT client ID is not valid UTF-8") from exc

        # This gateway only implements the CONNECT subset used by the bundled publisher.
        if protocol_id != 1 or flags & 0x08 or not client_id:
            self._send(address, CONNACK, bytes((RETURN_NOT_SUPPORTED,)))
            LOG.warning("Rejected unsupported MQTT-SN CONNECT from %s:%d", *address)
            return

        clean_session = bool(flags & 0x04)
        existing = self.sessions.get(address)
        if existing is None or clean_session or existing.client_id != client_id:
            self.sessions[address] = ClientSession(client_id=client_id)
        else:
            existing.last_seen = time.monotonic()

        self._send(address, CONNACK, bytes((RETURN_ACCEPTED,)))
        LOG.info(
            "MQTT-SN client connected: id=%s addr=%s:%d keep_alive=%d clean_session=%s",
            client_id,
            *address,
            keep_alive,
            clean_session,
        )

    def _handle_register(self, body: bytes, address: tuple[str, int]) -> None:
        session = self.sessions.get(address)
        if session is None:
            LOG.warning("Ignoring REGISTER from unconnected client %s:%d", *address)
            return
        if len(body) < 4:
            raise ValueError("REGISTER body is too short")

        message_id = int.from_bytes(body[2:4], "big")
        try:
            topic_name = body[4:].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("REGISTER topic name is not valid UTF-8") from exc

        if not topic_name or "\x00" in topic_name or "+" in topic_name or "#" in topic_name:
            self._send(
                address,
                REGACK,
                b"\x00\x00" + message_id.to_bytes(2, "big") + bytes((RETURN_NOT_SUPPORTED,)),
            )
            LOG.warning("Rejected invalid MQTT topic name from %s:%d", *address)
            return

        try:
            topic_id = session.register_topic(topic_name)
        except RuntimeError:
            self._send(
                address,
                REGACK,
                b"\x00\x00" + message_id.to_bytes(2, "big") + bytes((RETURN_INVALID_TOPIC_ID,)),
            )
            return

        regack = topic_id.to_bytes(2, "big") + message_id.to_bytes(2, "big") + bytes((RETURN_ACCEPTED,))
        self._send(address, REGACK, regack)
        LOG.info(
            "Registered MQTT-SN topic: client=%s topic=%s topic_id=%d",
            session.client_id,
            topic_name,
            topic_id,
        )

    def _handle_publish(self, body: bytes, address: tuple[str, int]) -> None:
        session = self.sessions.get(address)
        if session is None:
            LOG.warning("Ignoring PUBLISH from unconnected client %s:%d", *address)
            return
        if len(body) < 5:
            raise ValueError("PUBLISH body is too short")

        flags = body[0]
        topic_id_type = flags & 0x03
        qos_bits = flags & 0x60
        topic_id = int.from_bytes(body[1:3], "big")
        # QoS 0 uses a zero message ID; the current bridge supports only QoS 0.
        payload = body[5:]
        retain = bool(flags & 0x10)

        if topic_id_type != 0:
            LOG.warning("Unsupported topic ID type %d from %s:%d", topic_id_type, *address)
            return
        if qos_bits != 0:
            LOG.warning("Only MQTT-SN QoS 0 is supported; dropping packet from %s:%d", *address)
            return

        topic_name = session.id_to_topic.get(topic_id)
        if topic_name is None:
            LOG.warning("Unknown MQTT-SN topic ID %d from %s:%d", topic_id, *address)
            return

        accepted = self.bridge.publish(topic_name, payload, retain)
        if accepted:
            payload_text = payload.decode("utf-8", errors="replace")
            LOG.info(
                "Queued MQTT-SN -> MQTT: topic=%s content=%s retain=%s",
                topic_name,
                payload_text,
                retain,
            )
        else:
            LOG.warning("Message not forwarded to MQTT broker: topic=%s", topic_name)

    def _cleanup_sessions(self) -> None:
        cutoff = time.monotonic() - self.settings.session_timeout_seconds
        expired = [address for address, session in self.sessions.items() if session.last_seen < cutoff]
        for address in expired:
            session = self.sessions.pop(address)
            LOG.info("Expired idle MQTT-SN session: id=%s addr=%s:%d", session.client_id, *address)

    def serve(self, stop_event: Event) -> None:
        """Bind the UDP listener and handle packets until shutdown is requested."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_socket:
            self.socket = udp_socket
            udp_socket.bind((self.settings.bind_host, self.settings.bind_port))
            udp_socket.settimeout(1.0)
            LOG.info(
                "Listening for MQTT-SN over UDP on %s:%d",
                self.settings.bind_host,
                self.settings.bind_port,
            )
            try:
                while not stop_event.is_set():
                    try:
                        packet, address = udp_socket.recvfrom(65535)
                    except socket.timeout:
                        self._cleanup_sessions()
                        continue
                    try:
                        self.handle_datagram(packet, address)
                    except (ValueError, UnicodeDecodeError) as exc:
                        LOG.warning("Discarding invalid packet from %s:%d: %s", *address, exc)
                    except OSError:
                        LOG.exception("UDP I/O error while processing %s:%d", *address)
            finally:
                self.socket = None
                self.sessions.clear()


def main() -> int:
    """Start the MQTT broker bridge and UDP MQTT-SN gateway."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    config_path = os.getenv("CONFIG_PATH", "/app/config.yaml")
    try:
        settings = load_config(config_path)
    except ValueError as exc:
        LOG.error("Configuration error: %s", exc)
        return 2

    stop_event = Event()

    def handle_signal(signum: int, _frame: Any) -> None:
        LOG.info("Received signal %s; shutting down", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    bridge = MqttBrokerBridge(settings)
    bridge.start()
    gateway = MqttSnGateway(settings, bridge)
    try:
        gateway.serve(stop_event)
    except OSError as exc:
        LOG.error("Failed to start MQTT-SN UDP listener: %s", exc)
        return 1
    finally:
        bridge.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
