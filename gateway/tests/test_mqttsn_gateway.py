"""Tests for MQTT-SN packet handling and MQTT forwarding."""
from __future__ import annotations

import unittest

from mqttsn_gateway import (
    CONNACK,
    CONNECT,
    PINGREQ,
    PINGRESP,
    PUBLISH,
    REGISTER,
    REGACK,
    GatewaySettings,
    MqttSnGateway,
    decode_packet,
    encode_packet,
)


class FakeSocket:
    def __init__(self) -> None:
        self.sent: list[tuple[bytes, tuple[str, int]]] = []

    def sendto(self, packet: bytes, address: tuple[str, int]) -> int:
        self.sent.append((packet, address))
        return len(packet)


class FakeBridge:
    def __init__(self) -> None:
        self.messages: list[tuple[str, bytes, bool]] = []
        self.ready = True

    def publish(self, topic: str, payload: bytes, retain: bool) -> bool:
        if not self.ready:
            return False
        self.messages.append((topic, payload, retain))
        return True


class MqttSnGatewayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = GatewaySettings(
            bind_host="127.0.0.1",
            bind_port=1884,
            session_timeout_seconds=300,
            mqtt_host="localhost",
            mqtt_port=1883,
            mqtt_client_id="test-gateway",
            mqtt_keep_alive_seconds=60,
        )
        self.bridge = FakeBridge()
        self.gateway = MqttSnGateway(self.settings, self.bridge)  # type: ignore[arg-type]
        self.socket = FakeSocket()
        self.gateway.socket = self.socket
        self.address = ("127.0.0.1", 40000)

    def connect(self) -> None:
        body = bytes((0x04, 0x01)) + (60).to_bytes(2, "big") + b"test-client"
        self.gateway.handle_datagram(encode_packet(CONNECT, body), self.address)

    def register(self, topic: str, message_id: int = 1) -> int:
        body = b"\x00\x00" + message_id.to_bytes(2, "big") + topic.encode()
        self.gateway.handle_datagram(encode_packet(REGISTER, body), self.address)
        packet, _ = self.socket.sent[-1]
        packet_type, regack = decode_packet(packet)
        self.assertEqual(packet_type, REGACK)
        self.assertEqual(int.from_bytes(regack[2:4], "big"), message_id)
        self.assertEqual(regack[4], 0)
        return int.from_bytes(regack[0:2], "big")

    def test_encode_decode_short_and_extended_packets(self) -> None:
        short_packet = encode_packet(PINGREQ)
        self.assertEqual(decode_packet(short_packet), (PINGREQ, b""))

        extended_body = b"a" * 300
        extended_packet = encode_packet(PUBLISH, extended_body)
        self.assertEqual(decode_packet(extended_packet), (PUBLISH, extended_body))

    def test_decode_rejects_wrong_length(self) -> None:
        with self.assertRaisesRegex(ValueError, "length mismatch"):
            decode_packet(b"\x03\x16")

    def test_connect_returns_connack(self) -> None:
        self.connect()
        packet_type, body = decode_packet(self.socket.sent[-1][0])
        self.assertEqual(packet_type, CONNACK)
        self.assertEqual(body, b"\x00")
        self.assertIn(self.address, self.gateway.sessions)

    def test_register_and_publish_are_forwarded(self) -> None:
        self.connect()
        topic_id = self.register("lab/temperature")
        publish_body = b"\x10" + topic_id.to_bytes(2, "big") + b"\x00\x00" + b"23.40"
        self.gateway.handle_datagram(encode_packet(PUBLISH, publish_body), self.address)
        self.assertEqual(self.bridge.messages, [("lab/temperature", b"23.40", True)])

    def test_pingreq_returns_pingresp(self) -> None:
        self.connect()
        self.gateway.handle_datagram(encode_packet(PINGREQ), self.address)
        self.assertEqual(decode_packet(self.socket.sent[-1][0]), (PINGRESP, b""))

    def test_unknown_topic_id_is_not_forwarded(self) -> None:
        self.connect()
        publish_body = b"\x00\x00\x09\x00\x00value"
        self.gateway.handle_datagram(encode_packet(PUBLISH, publish_body), self.address)
        self.assertEqual(self.bridge.messages, [])

    def test_clean_session_clears_topic_registry(self) -> None:
        self.connect()
        self.register("lab/state")
        self.connect()
        self.assertEqual(self.gateway.sessions[self.address].topic_to_id, {})


if __name__ == "__main__":
    unittest.main()
