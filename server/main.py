import json
import os
import sys
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
import logging

import pika
from lxml import etree

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# Config
HOST = os.getenv("INTERNAL_HOST", "0.0.0.0")
PORT = int(os.getenv("EXTERNAL_PORT", "8080"))

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")


# TODO(nasr): throw this in an iterable array or config block later
CHECKIN_EXCHANGE = "user.checkin.topic"
CHECKIN_ROUTING_KEY = "routing.user.checkin"

HEARTBEAT_EXCHANGE = "heartbeat.direct"
HEARTBEAT_ROUTING_KEY = "routing.heartbeat"

CHECKIN_XSD_PATH = "./xsd/checkin.xsd"
HEARTBEAT_XSD_PATH = "./xsd/heartbeat.xsd"

CHECKIN_XSD_SCHEMA = None
HEARTBEAT_XSD_SCHEMA = None

# TODO(nasr):
PUBLISH_RETRIES = int(os.getenv("RABBITMQ_PUBLISH_RETRIES", "3"))
PUBLISH_RETRY_DELAY_SECONDS = float(
    os.getenv("RABBITMQ_PUBLISH_RETRY_DELAY_SECONDS", "0.5")
)


#########################################################################
# Helper functions
# XSD loading


def load_xsd_schema(path: str) -> etree.XMLSchema:
    logger.info("loading XSD schema from %s", path)

    with open(path, "rb") as f:
        xsd_doc = etree.parse(f)

    logger.info("successfully loaded schema: %s", path)
    return etree.XMLSchema(xsd_doc)


def get_rabbitmq_connection():
    credentials = pika.PlainCredentials(
        username=RABBITMQ_USER,
        password=RABBITMQ_PASS,
    )

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            credentials=credentials,
            heartbeat=600,  # 10 minute heartbeat
            blocked_connection_timeout=300,
            connection_attempts=3,
            retry_delay=2,
        )
    )
    return connection


def setup_rabbitmq():
    logger.info("initializing RabbitMQ exchanges")

    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()

        logger.info("declaring exchange: %s", CHECKIN_EXCHANGE)
        channel.exchange_declare(
            exchange=CHECKIN_EXCHANGE,
            exchange_type="topic",
            durable=True,
        )

        logger.info("declaring exchange: %s", HEARTBEAT_EXCHANGE)
        channel.exchange_declare(
            exchange=HEARTBEAT_EXCHANGE,
            exchange_type="direct",
            durable=True,
        )

        logger.info("RabbitMQ setup complete")
        connection.close()

    except Exception as e:
        logger.exception("Failed to initialize RabbitMQ: %s", e)
        raise


def _declare_required_exchanges(channel):
    channel.exchange_declare(
        exchange=CHECKIN_EXCHANGE,
        exchange_type="topic",
        durable=True,
    )
    channel.exchange_declare(
        exchange=HEARTBEAT_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )


def _publish_with_retry(exchange: str, routing_key: str, xml_bytes: bytes, kind: str):
    last_error = None

    for attempt in range(1, PUBLISH_RETRIES + 1):
        connection = None
        try:
            connection = get_rabbitmq_connection()
            channel = connection.channel()

            # Re-declare exchanges in case broker was restarted or topology changed.
            _declare_required_exchanges(channel)

            channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=xml_bytes,
                properties=pika.BasicProperties(
                    content_type="application/xml",
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                ),
            )

            logger.info("%s message published", kind)
            return

        except (pika.exceptions.AMQPError, OSError) as e:
            last_error = e
            if attempt >= PUBLISH_RETRIES:
                break

            delay = PUBLISH_RETRY_DELAY_SECONDS * attempt
            logger.warning(
                "%s publish attempt %d/%d failed: %s; retrying in %.2fs",
                kind,
                attempt,
                PUBLISH_RETRIES,
                e,
                delay,
            )
            time.sleep(delay)

        finally:
            if connection:
                try:
                    connection.close()
                except Exception as e:
                    logger.warning("Error closing connection: %s", e)

    logger.exception(
        "Failed to publish %s after %d attempts: %s",
        kind,
        PUBLISH_RETRIES,
        last_error,
    )
    raise last_error


#########################################################################


####################################################
# checkin building, validation, and publishing


def build_checkin_xml(data: dict) -> bytes:
    logger.info("building checkin XML for id=%s", data.get("id"))

    root = etree.Element("CheckIn")

    id_el = etree.SubElement(root, "id")
    id_el.text = str(data["id"])

    ts_el = etree.SubElement(root, "timestamp")
    # Ensure timestamps are timezone-aware so consumers expecting an offset can parse them
    ts_el.text = (
        data.get("timestamp")
        or datetime.now().astimezone().isoformat()
    )

    xml = etree.tostring(
        root,
        xml_declaration=True,
        encoding="UTF-8",
    )

    logger.info("checkin XML built successfully")

    return xml


def validate_checkin_xml(xml_bytes: bytes) -> bool:
    logger.info("validating checkin XML")

    try:
        doc = etree.fromstring(xml_bytes)
        valid = CHECKIN_XSD_SCHEMA.validate(doc)

        if valid:
            logger.info("checkin XML validation successful")
        else:
            logger.error("checkin XML validation failed")

        return valid

    except Exception as e:
        logger.exception("exception during checkin XML validation: %s", e)
        return False


def publish_checkin(xml_bytes: bytes):
    logger.info(
        "publishing checkin message exchange=%s routing_key=%s",
        CHECKIN_EXCHANGE,
        CHECKIN_ROUTING_KEY,
    )

    _publish_with_retry(
        exchange=CHECKIN_EXCHANGE,
        routing_key=CHECKIN_ROUTING_KEY,
        xml_bytes=xml_bytes,
        kind="checkin",
    )


#####################################################

####################################################
# heartbeat building, validation and publishing


def build_heartbeat_xml(data: dict) -> bytes:
    logger.info(
        "building heartbeat XML for service=%s",
        data.get("serviceId"),
    )

    root = etree.Element("Heartbeat")

    serv_id_el = etree.SubElement(root, "serviceId")
    serv_id_el.text = str(data["serviceId"])

    ts_el = etree.SubElement(root, "timestamp")
    # Ensure timestamps include timezone offset
    ts_el.text = (
        data.get("timestamp")
        or datetime.now().astimezone().isoformat()
    )

    xml = etree.tostring(
        root,
        xml_declaration=True,
        encoding="UTF-8",
    )

    logger.info("heartbeat XML built successfully")

    return xml


def validate_heartbeat_xml(xml_bytes: bytes) -> bool:
    logger.info("validating heartbeat XML")

    try:
        doc = etree.fromstring(xml_bytes)
        valid = HEARTBEAT_XSD_SCHEMA.validate(doc)

        if valid:
            logger.info("heartbeat XML validation successful")
        else:
            logger.error("heartbeat XML validation failed")

        return valid

    except Exception as e:
        logger.exception("exception during heartbeat XML validation: %s", e)
        return False


def publish_heartbeat(xml_bytes: bytes):
    logger.info(
        "publishing heartbeat exchange=%s routing_key=%s",
        HEARTBEAT_EXCHANGE,
        HEARTBEAT_ROUTING_KEY,
    )

    _publish_with_retry(
        exchange=HEARTBEAT_EXCHANGE,
        routing_key=HEARTBEAT_ROUTING_KEY,
        xml_bytes=xml_bytes,
        kind="heartbeat",
    )


def send_heartbeat():
    logger.info("sending heartbeat")

    data = build_heartbeat_xml(
        {
            "serviceId": "BADGE_SCANNER",
        }
    )

    validated = validate_heartbeat_xml(data)

    if validated is False:
        logger.error("failed to validate heartbeat")
        return

    publish_heartbeat(data)

    logger.info("heartbeat sent successfully")


#####################################################
# HTTP stuff


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        logger.info("received POST request path=%s", self.path)

        try:
            length = int(self.headers.get("Content-Length", 0))

            logger.info("reading request body length=%d", length)

            body = self.rfile.read(length)

            logger.info("parsing JSON payload")

            data = json.loads(body)

            logger.info("request payload parsed successfully")

            xml_bytes = build_checkin_xml(data)

            if not validate_checkin_xml(xml_bytes):
                logger.error("invalid checkin XML")
                self._respond(400, b"Invalid XML schema")
                return

            publish_checkin(xml_bytes)

            logger.info("request processed successfully")

            self._respond(200, b"OK")

        except json.JSONDecodeError:
            logger.exception("failed to decode JSON")
            self._respond(400, b"Invalid JSON")

        except Exception as e:
            logger.exception("unexpected server error: %s", e)
            self._respond(500, b"Internal Server Error")

    def _respond(self, code: int, message: bytes):
        logger.info("responding with status=%d", code)

        self.send_response(code)
        self.end_headers()
        self.wfile.write(message)

    def log_message(self, format, *args):
        return


#####################################################
# Server stuff


def listen():
    logger.info("starting HTTP server host=%s port=%d", HOST, PORT)

    server = HTTPServer((HOST, PORT), Handler)

    logger.info("HTTP server listening")

    server.serve_forever()


#####################################################


def main():
    global CHECKIN_XSD_SCHEMA
    global HEARTBEAT_XSD_SCHEMA

    logger.info("starting application")

    CHECKIN_XSD_SCHEMA = load_xsd_schema(CHECKIN_XSD_PATH)
    HEARTBEAT_XSD_SCHEMA = load_xsd_schema(HEARTBEAT_XSD_PATH)

    setup_rabbitmq()

    logger.info("initial setup complete")

    listen()

    return 0


if __name__ == "__main__":
    sys.exit(main())
