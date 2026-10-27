"""Minimal MQTT-SN v1.2 client for publishing QoS 0 messages over UDP.

This intentionally implements only the flow needed by this project:
CONNECT/CONNACK, REGISTER/REGACK, PUBLISH (QoS 0), PINGREQ/PINGRESP,
and DISCONNECT. It expects an MQTT-SN gateway at the configured address.
"""
from __future__ import annotations

import logging
import socket
import time
from typing import Optional

LOG = logging.getLogger(__name__)

# MQTT-SN v1.2 message types
CONNECT = 0x04
CONNACK = 0x05
REGISTER = 0x0A
REGACK = 0x0B
PUBLISH = 0x0C
PINGREQ = 0x16
PINGRESP = 0x17
DISCONNECT = 0x18

RETURN_CODE_NAMES = {
    0x00: "accepted",
    0x01: "rejected: congestion",
    0x02: "rejected: invalid topic ID",
    0x03: "rejected: not supported",
}


def encode_packet(message_type: int, body: bytes = b"") -> bytes:
    """Encode an MQTT-SN packet with a short or extended length header."""
    if not 0 <= message_type <= 0xFF:
        raise ValueError("message_type must fit in one byte")

    short_length = len(body) + 2  # length + message type + body
    if short_length <= 0xFF:
        return bytes((short_length, message_type)) + body

    extended_length = len(body) + 4  # 0x01 + uint16 length + type + body
    if extended_length > 0xFFFF:
        raise ValueError("MQTT-SN packet exceeds the 65535-byte protocol limit")
    return b"\x01" + extended_length.to_bytes(2, "big") + bytes((message_type,)) + body


def decode_packet(packet: bytes) -> tuple[int, bytes]:
    """Return (message_type, body), validating MQTT-SN length framing."""
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


class MqttSnClient:
    """A small UDP MQTT-SN client supporting one active gateway connection."""

    def __init__(
        self,
        host: str,
        port: int,
        client_id: str,
        timeout_seconds: float = 5.0,
        keep_alive_seconds: int = 60,
    ) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self.timeout_seconds = float(timeout_seconds)
        self.keep_alive_seconds = int(keep_alive_seconds)
        self.sock: Optional[socket.socket] = None
        self.connected = False
        self.last_send_monotonic = 0.0
        self._message_id = 0

    def _next_message_id(self) -> int:
        self._message_id = (self._message_id % 0xFFFF) + 1
        return self._message_id

    def _send(self, packet: bytes) -> None:
        if self.sock is None:
            raise RuntimeError("UDP socket is not open")
        self.sock.send(packet)
        self.last_send_monotonic = time.monotonic()

    def _receive(self, timeout: float) -> tuple[int, bytes]:
        if self.sock is None:
            raise RuntimeError("UDP socket is not open")
        self.sock.settimeout(max(0.001, timeout))
        packet = self.sock.recv(65535)
        return decode_packet(packet)

    def _wait_for(self, expected_type: int, timeout: Optional[float] = None) -> bytes:
        deadline = time.monotonic() + (self.timeout_seconds if timeout is None else timeout)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"Timed out waiting for MQTT-SN message type 0x{expected_type:02x}"
                )
            try:
                message_type, body = self._receive(remaining)
            except socket.timeout as exc:
                raise TimeoutError(
                    f"Timed out waiting for MQTT-SN message type 0x{expected_type:02x}"
                ) from exc
            if message_type == expected_type:
                return body
            LOG.debug("Ignoring MQTT-SN message type 0x%02x while waiting", message_type)

    def connect(self) -> None:
        client_id_bytes = self.client_id.encode("utf-8")
        if not client_id_bytes:
            raise ValueError("client_id must not be empty")
        if len(client_id_bytes) > 0xFFFF - 6:
            raise ValueError("client_id is too long for an MQTT-SN CONNECT packet")
        if not 0 <= self.keep_alive_seconds <= 0xFFFF:
            raise ValueError("keep_alive_seconds must be between 0 and 65535")

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.connect((self.host, self.port))

        # CONNECT flags: cleanSession=1; MQTT-SN protocol ID is 1 (v1.2).
        body = (
            bytes((0x04, 0x01))
            + self.keep_alive_seconds.to_bytes(2, "big")
            + client_id_bytes
        )
        self._send(encode_packet(CONNECT, body))
        connack = self._wait_for(CONNACK)
        if len(connack) != 1:
            raise RuntimeError(f"Malformed CONNACK body: {connack.hex()}")
        if connack[0] != 0:
            reason = RETURN_CODE_NAMES.get(connack[0], f"return code {connack[0]}")
            raise RuntimeError(f"MQTT-SN gateway rejected CONNECT: {reason}")

        self.connected = True
        LOG.info("Connected to MQTT-SN gateway %s:%s as %s", self.host, self.port, self.client_id)

    def register_topic(self, topic: str) -> int:
        if not self.connected:
            raise RuntimeError("Not connected to an MQTT-SN gateway")
        topic_bytes = topic.encode("utf-8")
        if not topic_bytes or b"\x00" in topic_bytes:
            raise ValueError(f"Invalid topic name: {topic!r}")

        message_id = self._next_message_id()
        # A client sends topicId=0 in REGISTER; the gateway assigns the topic ID.
        body = b"\x00\x00" + message_id.to_bytes(2, "big") + topic_bytes
        self._send(encode_packet(REGISTER, body))
        regack = self._wait_for(REGACK)
        if len(regack) != 5:
            raise RuntimeError(f"Malformed REGACK for topic {topic!r}: {regack.hex()}")

        topic_id = int.from_bytes(regack[0:2], "big")
        received_message_id = int.from_bytes(regack[2:4], "big")
        return_code = regack[4]
        if received_message_id != message_id:
            raise RuntimeError(
                f"REGACK message ID mismatch for {topic!r}: "
                f"expected {message_id}, received {received_message_id}"
            )
        if return_code != 0:
            reason = RETURN_CODE_NAMES.get(return_code, f"return code {return_code}")
            raise RuntimeError(f"MQTT-SN gateway rejected topic {topic!r}: {reason}")
        if topic_id == 0:
            raise RuntimeError(f"MQTT-SN gateway assigned invalid topic ID 0 to {topic!r}")

        LOG.info("Registered topic %s (topic ID %d)", topic, topic_id)
        return topic_id

    def publish(self, topic_id: int, payload: str, retain: bool = False) -> None:
        if not self.connected:
            raise RuntimeError("Not connected to an MQTT-SN gateway")
        if not 1 <= topic_id <= 0xFFFF:
            raise ValueError("topic_id must be between 1 and 65535")
        payload_bytes = payload.encode("utf-8")
        # QoS 0, normal topic ID, optional RETAIN; message ID is 0 for QoS 0.
        flags = 0x10 if retain else 0x00
        body = bytes((flags,)) + topic_id.to_bytes(2, "big") + b"\x00\x00" + payload_bytes
        self._send(encode_packet(PUBLISH, body))

    def ping(self) -> None:
        if not self.connected:
            return
        self._send(encode_packet(PINGREQ))
        body = self._wait_for(PINGRESP, timeout=self.timeout_seconds)
        if body:
            LOG.debug("PINGRESP had an unexpected body: %s", body.hex())

    def disconnect(self) -> None:
        if self.sock is None:
            return
        try:
            if self.connected:
                self._send(encode_packet(DISCONNECT))
        except OSError:
            LOG.debug("Could not send DISCONNECT", exc_info=True)
        finally:
            self.connected = False
            self.sock.close()
            self.sock = None
            LOG.info("Disconnected from MQTT-SN gateway")
