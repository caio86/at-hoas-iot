"""Config-driven MQTT-SN publisher with Home Assistant MQTT discovery."""

from __future__ import annotations

import json
import logging
import math
import os
import random
import re
import signal
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import Any, Callable

import yaml
from mqttsn_client import MqttSnClient

LOG = logging.getLogger("mqttsn_publisher")


@dataclass(frozen=True)
class ConnectionConfig:
    """Settings required to connect to an MQTT-SN gateway."""

    host: str
    port: int
    client_id: str
    timeout_seconds: float = 5.0
    keep_alive_seconds: int = 60


@dataclass(frozen=True)
class PublishConfig:
    """Publishing cadence and reconnect behavior."""

    interval_seconds: float = 5.0
    reconnect_delay_seconds: float = 5.0


@dataclass(frozen=True)
class DiscoveryConfig:
    """Home Assistant MQTT discovery settings."""

    enabled: bool = True
    prefix: str = "homeassistant"
    device_identifier: str = "mqttsn_simulated_lab"
    device_name: str = "MQTT-SN Simulated Lab"
    manufacturer: str = "MQTT-SN Lab"
    model: str = "Random Telemetry Publisher"


@dataclass(frozen=True)
class TopicConfig:
    """Value-generation, MQTT publication, and optional entity metadata."""

    topic: str
    datatype: str
    mode: str
    minimum: int | float | None = None
    maximum: int | float | None = None
    decimals: int = 2
    retain: bool = False
    ha_name: str | None = None
    ha_component: str | None = None
    ha_device_class: str | None = None
    ha_unit: str | None = None
    ha_state_class: str | None = None
    ha_icon: str | None = None

    def generate_value(self) -> int | float:
        """Generate one value according to this topic's configuration."""

        if self.mode == "on_off":
            return random.randint(0, 1)

        if self.mode == "percentage":
            if self.datatype == "int":
                return random.randint(0, 100)

            return round(
                random.uniform(0.0, 100.0),
                self.decimals,
            )

        if self.mode == "range":
            # load_config guarantees these bounds are configured and valid.
            assert self.minimum is not None and self.maximum is not None

            if self.datatype == "int":
                return random.randint(
                    int(self.minimum),
                    int(self.maximum),
                )

            return round(
                random.uniform(
                    float(self.minimum),
                    float(self.maximum),
                ),
                self.decimals,
            )

        raise ValueError(f"Unsupported generation mode: {self.mode}")

    def format_value(self, value: int | float) -> str:
        """Format a generated value as the text payload sent over MQTT-SN."""

        if self.datatype == "int":
            return str(int(value))

        return f"{float(value):.{self.decimals}f}"


@dataclass(frozen=True)
class PublisherConfig:
    """Validated application configuration."""

    connection: ConnectionConfig
    publish: PublishConfig
    topics: tuple[TopicConfig, ...]
    discovery: DiscoveryConfig = DiscoveryConfig()


def _require_mapping(value: Any, location: str) -> dict[str, Any]:
    """Validate that a configuration value is a mapping."""

    if not isinstance(value, dict):
        raise ValueError(f"{location} must be a mapping/object")

    return value


def _require_number(
    value: Any,
    location: str,
    *,
    integer: bool = False,
) -> int | float:
    """Validate a numeric configuration value."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{location} must be a number")

    if not math.isfinite(float(value)):
        raise ValueError(f"{location} must be finite")

    if integer and not isinstance(value, int):
        raise ValueError(f"{location} must be an integer when datatype is 'int'")

    return value


def _require_integer(
    value: Any,
    location: str,
    minimum: int,
    maximum: int,
) -> int:
    """Validate an integer and its allowed boundaries."""

    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise ValueError(
            f"{location} must be an integer between {minimum} and {maximum}"
        )

    return value


def _optional_string(
    mapping: dict[str, Any],
    key: str,
    location: str,
) -> str | None:
    """Validate an optional, non-empty string."""

    value = mapping.get(key)

    if value is None:
        return None

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{location}.{key} must be a non-empty string when provided")

    return value.strip()


def _load_yaml(path: str | Path) -> dict[str, Any]:
    """Read and validate the YAML configuration file."""

    try:
        with open(path, "r", encoding="utf-8") as stream:
            raw = yaml.safe_load(stream)

    except FileNotFoundError as exc:
        raise ValueError(f"Config file not found: {path}") from exc

    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML in config file {path}: {exc}") from exc

    return _require_mapping(raw, "Config root")


def _parse_connection(raw: Any) -> ConnectionConfig:
    """Parse and validate gateway connection settings."""

    connection = _require_mapping(raw, "connection")

    host = connection.get("host")

    if not isinstance(host, str) or not host.strip():
        raise ValueError("connection.host must be a non-empty string")

    client_id = connection.get("client_id")

    if not isinstance(client_id, str) or not client_id.strip():
        raise ValueError("connection.client_id must be a non-empty string")

    port = _require_integer(
        connection.get("port", 1884),
        "connection.port",
        1,
        65535,
    )

    timeout = _require_number(
        connection.get("timeout_seconds", 5),
        "connection.timeout_seconds",
    )

    if timeout <= 0:
        raise ValueError("connection.timeout_seconds must be greater than 0")

    keep_alive = _require_integer(
        connection.get("keep_alive_seconds", 60),
        "connection.keep_alive_seconds",
        0,
        65535,
    )

    return ConnectionConfig(
        host=host.strip(),
        port=port,
        client_id=client_id.strip(),
        timeout_seconds=float(timeout),
        keep_alive_seconds=keep_alive,
    )


def _parse_publish(raw: Any) -> PublishConfig:
    """Parse and validate publishing and reconnection settings."""

    publish = _require_mapping(raw, "publish")

    interval = _require_number(
        publish.get("interval_seconds", 5),
        "publish.interval_seconds",
    )

    if interval <= 0:
        raise ValueError("publish.interval_seconds must be greater than 0")

    reconnect_delay = _require_number(
        publish.get("reconnect_delay_seconds", 5),
        "publish.reconnect_delay_seconds",
    )

    if reconnect_delay < 0:
        raise ValueError("publish.reconnect_delay_seconds must be zero or greater")

    return PublishConfig(
        interval_seconds=float(interval),
        reconnect_delay_seconds=float(reconnect_delay),
    )


def _parse_discovery(raw: Any) -> DiscoveryConfig:
    """Parse Home Assistant MQTT discovery settings."""

    if raw is None:
        return DiscoveryConfig()

    discovery = _require_mapping(raw, "discovery")

    enabled = discovery.get("enabled", True)

    if not isinstance(enabled, bool):
        raise ValueError("discovery.enabled must be true or false")

    prefix = discovery.get("prefix", "homeassistant")

    if (
        not isinstance(prefix, str)
        or not prefix.strip()
        or "+" in prefix
        or "#" in prefix
        or "\x00" in prefix
        or prefix.startswith("/")
        or prefix.endswith("/")
    ):
        raise ValueError("discovery.prefix must be a valid MQTT topic prefix")

    device_raw = discovery.get("device", {})
    device = _require_mapping(
        device_raw,
        "discovery.device",
    )

    defaults = DiscoveryConfig()

    device_identifier = device.get(
        "identifier",
        defaults.device_identifier,
    )
    device_name = device.get(
        "name",
        defaults.device_name,
    )
    manufacturer = device.get(
        "manufacturer",
        defaults.manufacturer,
    )
    model = device.get(
        "model",
        defaults.model,
    )

    values = {
        "identifier": device_identifier,
        "name": device_name,
        "manufacturer": manufacturer,
        "model": model,
    }

    for key, value in values.items():
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"discovery.device.{key} must be a non-empty string")

    return DiscoveryConfig(
        enabled=enabled,
        prefix=prefix.strip(),
        device_identifier=device_identifier.strip(),
        device_name=device_name.strip(),
        manufacturer=manufacturer.strip(),
        model=model.strip(),
    )


def _parse_topic(raw: Any, index: int) -> TopicConfig:
    """Parse and validate a single topic configuration."""

    location = f"topics[{index}]"
    topic = _require_mapping(raw, location)

    name = topic.get("topic")

    if not isinstance(name, str) or not name.strip() or "\x00" in name:
        raise ValueError(
            f"{location}.topic must be a non-empty string without NUL characters"
        )

    datatype = topic.get("datatype")

    if datatype not in ("int", "float"):
        raise ValueError(f"{location}.datatype must be 'int' or 'float'")

    generation = _require_mapping(
        topic.get("generation"),
        f"{location}.generation",
    )

    mode = generation.get("mode")

    if mode not in ("on_off", "percentage", "range"):
        raise ValueError(
            f"{location}.generation.mode must be 'on_off', 'percentage', or 'range'"
        )

    decimals = _require_integer(
        generation.get("decimals", 2),
        f"{location}.generation.decimals",
        0,
        10,
    )

    minimum: int | float | None = None
    maximum: int | float | None = None

    if mode == "on_off":
        if datatype != "int":
            raise ValueError(
                f"{location}: on_off requires datatype: int (values are 0 or 1)"
            )

    elif mode == "range":
        if "min" not in generation or "max" not in generation:
            raise ValueError(
                f"{location}.generation for range mode requires min and max"
            )

        integer = datatype == "int"

        minimum = _require_number(
            generation["min"],
            f"{location}.generation.min",
            integer=integer,
        )

        maximum = _require_number(
            generation["max"],
            f"{location}.generation.max",
            integer=integer,
        )

        if minimum > maximum:
            raise ValueError(
                f"{location}.generation.min must be less than or equal to max"
            )

    retain = topic.get("retain", False)

    if not isinstance(retain, bool):
        raise ValueError(f"{location}.retain must be true or false")

    ha = _require_mapping(
        topic.get("home_assistant", {}),
        f"{location}.home_assistant",
    )

    component = _optional_string(
        ha,
        "component",
        f"{location}.home_assistant",
    )

    if component is not None and component not in (
        "sensor",
        "binary_sensor",
    ):
        raise ValueError(
            f"{location}.home_assistant.component must be 'sensor' or 'binary_sensor'"
        )

    if component == "binary_sensor" and (datatype != "int" or mode != "on_off"):
        raise ValueError(
            f"{location}.home_assistant.component=binary_sensor "
            "requires datatype: int and mode: on_off"
        )

    return TopicConfig(
        topic=name.strip(),
        datatype=datatype,
        mode=mode,
        minimum=minimum,
        maximum=maximum,
        decimals=decimals,
        retain=retain,
        ha_name=_optional_string(
            ha,
            "name",
            f"{location}.home_assistant",
        ),
        ha_component=component,
        ha_device_class=_optional_string(
            ha,
            "device_class",
            f"{location}.home_assistant",
        ),
        ha_unit=_optional_string(
            ha,
            "unit_of_measurement",
            f"{location}.home_assistant",
        ),
        ha_state_class=_optional_string(
            ha,
            "state_class",
            f"{location}.home_assistant",
        ),
        ha_icon=_optional_string(
            ha,
            "icon",
            f"{location}.home_assistant",
        ),
    )


def load_config(path: str | Path) -> PublisherConfig:
    """Load the YAML file and return a fully validated configuration."""

    raw = _load_yaml(path)

    raw_topics = raw.get("topics")

    if not isinstance(raw_topics, list) or not raw_topics:
        raise ValueError("topics must be a non-empty list")

    topics = tuple(_parse_topic(topic, index) for index, topic in enumerate(raw_topics))

    seen_names: set[str] = set()
    duplicates: set[str] = set()

    for topic in topics:
        if topic.topic in seen_names:
            duplicates.add(topic.topic)

        seen_names.add(topic.topic)

    if duplicates:
        names = ", ".join(repr(name) for name in sorted(duplicates))

        raise ValueError(f"Duplicate topic configured: {names}")

    return PublisherConfig(
        connection=_parse_connection(raw.get("connection")),
        publish=_parse_publish(raw.get("publish", {})),
        topics=topics,
        discovery=_parse_discovery(raw.get("discovery")),
    )


def _slug(value: str) -> str:
    """Return a stable, Home Assistant-friendly identifier."""

    slug = re.sub(
        r"[^a-zA-Z0-9_]+",
        "_",
        value.strip().lower(),
    ).strip("_")

    slug = re.sub(r"_+", "_", slug)

    return slug or "mqttsn_entity"


def _friendly_name(topic_name: str) -> str:
    """Make a readable entity name from the final topic segments."""

    parts = topic_name.split("/")[-2:]

    return " ".join(part.replace("_", " ").replace("-", " ").title() for part in parts)


def build_discovery_config(
    topic: TopicConfig,
    discovery: DiscoveryConfig,
    client_id: str,
) -> tuple[str, str]:
    """Build the discovery topic and retained JSON payload."""

    component = topic.ha_component or (
        "binary_sensor" if topic.mode == "on_off" else "sensor"
    )

    unique_id = _slug(f"{client_id}_{topic.topic}")

    config_topic = f"{discovery.prefix}/{component}/{unique_id}/config"

    entity: dict[str, Any] = {
        "name": topic.ha_name or _friendly_name(topic.topic),
        "unique_id": unique_id,
        "state_topic": topic.topic,
        "origin": {
            "name": "MQTT-SN Random Telemetry Publisher",
            "sw_version": "1.0",
        },
        "device": {
            "identifiers": [
                discovery.device_identifier,
            ],
            "name": discovery.device_name,
            "manufacturer": discovery.manufacturer,
            "model": discovery.model,
        },
    }

    if component == "binary_sensor":
        entity["payload_on"] = "1"
        entity["payload_off"] = "0"

    else:
        device_class = topic.ha_device_class
        lower_name = topic.topic.lower()

        if device_class is None:
            if "temperature" in lower_name or lower_name.endswith("_temp"):
                device_class = "temperature"

            elif "humidity" in lower_name:
                device_class = "humidity"

        if device_class:
            entity["device_class"] = device_class

        unit = topic.ha_unit

        if unit is None:
            if device_class == "temperature":
                unit = "°C"

            elif device_class == "humidity" or "percent" in lower_name:
                unit = "%"

        if unit:
            entity["unit_of_measurement"] = unit

        state_class = topic.ha_state_class or "measurement"

        if state_class:
            entity["state_class"] = state_class

    if topic.ha_icon:
        entity["icon"] = topic.ha_icon

    payload = json.dumps(
        entity,
        ensure_ascii=False,
        separators=(",", ":"),
    )

    return config_topic, payload


def build_discovery_configs(
    config: PublisherConfig,
) -> list[tuple[str, str]]:
    """Return all Home Assistant discovery topic/payload pairs."""

    if not config.discovery.enabled:
        return []

    return [
        build_discovery_config(
            topic,
            config.discovery,
            config.connection.client_id,
        )
        for topic in config.topics
    ]


def _publish_discovery(
    client: MqttSnClient,
    registered_discovery: list[tuple[str, int, str]],
) -> None:
    """Publish Home Assistant discovery documents as retained MQTT messages."""

    for config_topic, topic_id, payload in registered_discovery:
        client.publish(
            topic_id,
            payload,
            retain=True,
        )

        LOG.info(
            "Sent Home Assistant discovery: topic=%s content=%s retain=True",
            config_topic,
            payload,
        )


def _publish_cycle(
    client: MqttSnClient,
    registered_topics: list[tuple[TopicConfig, int]],
) -> None:
    """Generate and send one payload to every registered state topic."""

    for topic_config, topic_id in registered_topics:
        payload = topic_config.format_value(topic_config.generate_value())

        client.publish(
            topic_id,
            payload,
            retain=topic_config.retain,
        )

        LOG.info(
            "Sent MQTT-SN message: topic=%s content=%s",
            topic_config.topic,
            payload,
        )


def _publish_until_stopped(
    client: MqttSnClient,
    config: PublisherConfig,
    registered_topics: list[tuple[TopicConfig, int]],
    stop_event: Event,
) -> None:
    """Publish on schedule and handle MQTT-SN keep-alive traffic."""

    next_publish = time.monotonic()
    connection = config.connection
    interval = config.publish.interval_seconds

    while not stop_event.is_set():
        now = time.monotonic()

        if now >= next_publish:
            _publish_cycle(
                client,
                registered_topics,
            )

            next_publish = time.monotonic() + interval
            continue

        keep_alive = connection.keep_alive_seconds

        ping_interval = max(1.0, keep_alive / 2) if keep_alive > 0 else 0

        if ping_interval and now - client.last_send_monotonic >= ping_interval:
            client.ping()
            continue

        sleep_for = min(
            0.5,
            max(0.0, next_publish - now),
        )

        stop_event.wait(sleep_for)


def run(
    config: PublisherConfig,
    stop_event: Event,
    client_factory: Callable[..., MqttSnClient] = MqttSnClient,
) -> None:
    """Run the publisher, reconnecting to the gateway after session failures."""

    connection = config.connection

    while not stop_event.is_set():
        client: MqttSnClient | None = None

        try:
            client = client_factory(
                host=connection.host,
                port=connection.port,
                client_id=connection.client_id,
                timeout_seconds=connection.timeout_seconds,
                keep_alive_seconds=connection.keep_alive_seconds,
            )

            client.connect()

            registered_topics = [
                (
                    topic,
                    client.register_topic(topic.topic),
                )
                for topic in config.topics
            ]

            registered_discovery: list[tuple[str, int, str]] = []

            for discovery_topic, payload in build_discovery_configs(config):
                topic_id = client.register_topic(discovery_topic)

                registered_discovery.append(
                    (
                        discovery_topic,
                        topic_id,
                        payload,
                    )
                )

            # Config payloads are retained by the broker. Home Assistant can
            # discover the entities even if it starts after this publisher.
            _publish_discovery(
                client,
                registered_discovery,
            )

            _publish_until_stopped(
                client,
                config,
                registered_topics,
                stop_event,
            )

        except (
            OSError,
            TimeoutError,
            RuntimeError,
            ValueError,
        ) as exc:
            if not stop_event.is_set():
                LOG.error(
                    "Publisher connection/session failed: %s",
                    exc,
                )

        finally:
            if client is not None:
                client.disconnect()

        if not stop_event.is_set():
            delay = config.publish.reconnect_delay_seconds

            LOG.info(
                "Retrying MQTT-SN connection in %.1f seconds",
                delay,
            )

            stop_event.wait(delay)


def main() -> int:
    """Configure logging and signal handling, then start the publisher."""

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    stop_event = Event()

    def handle_signal(
        signum: int,
        _frame: Any,
    ) -> None:
        LOG.info(
            "Received signal %s; shutting down",
            signum,
        )

        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    config_path = os.getenv(
        "CONFIG_PATH",
        "/app/config.yaml",
    )

    try:
        config = load_config(config_path)

    except ValueError as exc:
        LOG.error(
            "Configuration error: %s",
            exc,
        )

        return 2

    LOG.info(
        "Loaded %d topic(s) from %s; Home Assistant discovery=%s",
        len(config.topics),
        config_path,
        "enabled" if config.discovery.enabled else "disabled",
    )

    try:
        run(
            config,
            stop_event,
        )

    except KeyboardInterrupt:
        stop_event.set()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
