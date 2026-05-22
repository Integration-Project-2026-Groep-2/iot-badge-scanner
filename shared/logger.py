"""Shared logging helpers for the client and server applications."""

from __future__ import annotations

import logging
import os
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

import pika


DEFAULT_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

_LOGGING_CONFIGURED = False


def configure_logging(level: int | None = None) -> None:
    """Configure the root logger once for the whole process."""

    global _LOGGING_CONFIGURED

    if _LOGGING_CONFIGURED:
        return

    if level is None:
        level_name = os.getenv("LOG_LEVEL", "INFO").upper()
        level = getattr(logging, level_name, logging.INFO)

    logging.basicConfig(level=level, format=DEFAULT_LOG_FORMAT)
    _LOGGING_CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Return a logger with shared default configuration."""

    configure_logging()
    return logging.getLogger(name)


class SeverityType(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass(slots=True)
class LogEvent:
    service: str
    level: SeverityType
    timestamp: datetime
    data: str

    def to_xml(self) -> str:
        root = ET.Element("LogEvent")

        service_elem = ET.SubElement(root, "service")
        service_elem.text = self.service

        level_elem = ET.SubElement(root, "level")
        level_elem.text = self.level.value

        timestamp_elem = ET.SubElement(root, "timestamp")
        timestamp_elem.text = self.timestamp.astimezone().isoformat()

        data_elem = ET.SubElement(root, "data")
        data_elem.text = self.data

        return ET.tostring(root, encoding="unicode")


LOGGER_CONFIG = {
    "exchange": "logs.direct",
    "exchange_type": "direct",
    "routing_key": "routing.log",
    "durable": True,
}


class RabbitMQLogger:
    def __init__(
        self,
        service_name: str,
        channel: pika.adapters.blocking_connection.BlockingChannel,
    ):
        self.service_name = service_name
        self.channel = channel
        self._setup_exchange()

    def _setup_exchange(self) -> None:
        try:
            self.channel.exchange_declare(
                exchange=LOGGER_CONFIG["exchange"],
                exchange_type=LOGGER_CONFIG["exchange_type"],
                durable=LOGGER_CONFIG["durable"],
            )
        except pika.exceptions.ChannelClosed as e:
            raise RuntimeError(f"Failed to declare exchange: {e}") from e

    def _publish(self, severity: SeverityType, data: str) -> None:
        event = LogEvent(
            service=self.service_name,
            level=severity,
            timestamp=datetime.now().astimezone(),
            data=data,
        )

        try:
            self.channel.basic_publish(
                exchange=LOGGER_CONFIG["exchange"],
                routing_key=LOGGER_CONFIG["routing_key"],
                body=event.to_xml().encode("utf-8"),
            )
        except pika.exceptions.ChannelClosed as e:
            raise RuntimeError(f"Failed to publish log: {e}") from e

    def debug(self, msg: str) -> None:
        self._publish(SeverityType.DEBUG, msg)

    def info(self, msg: str) -> None:
        self._publish(SeverityType.INFO, msg)

    def warning(self, msg: str) -> None:
        self._publish(SeverityType.WARNING, msg)

    def error(self, msg: str) -> None:
        self._publish(SeverityType.ERROR, msg)

    def critical(self, msg: str) -> None:
        self._publish(SeverityType.CRITICAL, msg)


def publish_log(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    service: str,
    severity: SeverityType,
    data: str,
) -> None:
    """Publish one structured log event."""

    event = LogEvent(
        service=service,
        level=severity,
        timestamp=datetime.now().astimezone(),
        data=data,
    )

    channel.basic_publish(
        exchange=LOGGER_CONFIG["exchange"],
        routing_key=LOGGER_CONFIG["routing_key"],
        body=event.to_xml().encode("utf-8"),
    )
