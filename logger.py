from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import xml.etree.ElementTree as ET
import pika
from typing import Optional

class SeverityType(str, Enum):
    """Log severity levels"""
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
        """Convert LogEvent to XML string"""
        root = ET.Element("LogEvent")

        service_elem = ET.SubElement(root, "service")
        service_elem.text = self.service

        level_elem = ET.SubElement(root, "level")
        level_elem.text = self.level.value  # Enum -> string

        timestamp_elem = ET.SubElement(root, "timestamp")
        timestamp_elem.text = self.timestamp.isoformat()

        data_elem = ET.SubElement(root, "data")
        data_elem.text = self.data

        return ET.tostring(root, encoding="unicode")



LOGGER_CONFIG = {
    "exchange": "logs.direct",
    "exchange_type": "direct",
    "routing_key": "routing.log",
    "durable": True,
}



class Logger:

    def __init__(self, service_name: str, channel: pika.adapters.blocking_connection.BlockingChannel):
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
            raise RuntimeError(f"Failed to declare exchange: {e}")

    def _publish(self, severity: SeverityType, data: str) -> None:
        # Create event
        event = LogEvent(
            service=self.service_name,
            level=severity,
            timestamp=datetime.now(),
            data=data,
        )

        # Convert to XML
        xml_str = event.to_xml()

        # Publish to RabbitMQ (as bytes)
        try:
            self.channel.basic_publish(
                exchange=LOGGER_CONFIG["exchange"],
                routing_key=LOGGER_CONFIG["routing_key"],
                body=xml_str.encode("utf-8"),
            )
        except pika.exceptions.ChannelClosed as e:
            print(f"Failed to publish log: {e}")
    # Convenience methods
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


async def log(channel: pika.adapters.blocking_connection.BlockingChannel,
              service: str, severity: SeverityType, data: str) -> None:
    # Create event
    event = LogEvent(
        service=service,
        level=severity,
        timestamp=datetime.now(),
        data=data,
    )

    # Serialize to XML
    xml_str = event.to_xml()

    # Publish
    try:
        channel.basic_publish(
            exchange=LOGGER_CONFIG["exchange"],
            routing_key=LOGGER_CONFIG["routing_key"],
            body=xml_str.encode("utf-8"),
        )
    except Exception as e:
        print(f"Failed to publish log: {e}")



if __name__ == "__main__":
    pass
