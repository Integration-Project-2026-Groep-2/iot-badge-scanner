import asyncio
import json
import os
import sqlite3
import sys
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Lock, Thread

import pika
from lxml import etree
from pika.exceptions import AMQPConnectionError, AMQPError

# Add root to path for shared imports
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from shared.logger import get_logger

logger = get_logger(__name__)

SERVICE_TAG = "iot-badge-scanner"

# HTTP Configuration
HOST = os.getenv("INTERNAL_HOST", "0.0.0.0")
PORT = int(os.getenv("EXTERNAL_PORT", "8080"))

# RabbitMQ Configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")

# Feature Flags
# KASSA_SIGN_IN_MODE = os.getenv("KASSA_SIGN_IN_MODE", "false").lower() == "true"
KASSA_SIGN_IN_MODE = True

# RabbitMQ Exchange & Routing Configuration
HEARTBEAT_EXCHANGE = "heartbeat.direct"
HEARTBEAT_ROUTING_KEY = "routing.heartbeat"

CHECKIN_EXCHANGE = "user.checkin.topic"
CHECKIN_CONTROLROOM_ROUTING_KEY = "routing.controlroom.user.checkin"
CHECKIN_KASSA_ROUTING_KEY = "routing.user.checkin"

CRM_USER_CONFIRMED_EXCHANGE = "contact.topic"
CRM_USER_CONFIRMED_QUEUE = "badgescanner.user.confirmed"
CRM_USER_CONFIRMED_ROUTING_KEY = "crm.user.confirmed"

KASSA_AUTHENTICATE_EXCHANGE = "kassa.direct"
KASSA_AUTHENTICATE_ROUTING_KEY = "routing.kassa.authenticate"

# XSD Schema Paths
CHECKIN_XSD_PATH = "./xsd/checkin.xsd"
HEARTBEAT_XSD_PATH = "./xsd/heartbeat.xsd"

# Retry Configuration
PUBLISH_RETRIES = int(os.getenv("RABBITMQ_PUBLISH_RETRIES", "3"))
PUBLISH_RETRY_DELAY_SECONDS = float(
    os.getenv("RABBITMQ_PUBLISH_RETRY_DELAY_SECONDS", "0.5")
)

# SQLite Database
DB_PATH = "users_muuid_table"

# Global State

CHECKIN_XSD_SCHEMA = None
HEARTBEAT_XSD_SCHEMA = None
db_lock = Lock()


# Database Management


class UserDatabase:
    """Manages user UUID persistence."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize the database and users table."""
        try:
            with sqlite3.connect(self.db_path) as con:
                cursor = con.cursor()
                cursor.execute(
                    "CREATE TABLE IF NOT EXISTS users (muuid TEXT PRIMARY KEY)"
                )
                con.commit()
            logger.info("Database initialized: %s", self.db_path)
        except sqlite3.Error as e:
            logger.exception("Failed to initialize database: %s", e)
            raise

    def user_exists(self, muuid: str) -> bool:
        """Check if user already exists in the database."""
        try:
            with sqlite3.connect(self.db_path) as con:
                cursor = con.cursor()
                cursor.execute("SELECT 1 FROM users WHERE muuid = ?", (muuid,))
                return cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.exception("Database query failed: %s", e)
            return False

    def add_user(self, muuid: str) -> bool:
        """Add a new user to the database."""
        try:
            with sqlite3.connect(self.db_path) as con:
                cursor = con.cursor()
                cursor.execute("INSERT INTO users (muuid) VALUES (?)", (muuid,))
                con.commit()
            logger.info("User added to database: %s", muuid)
            return True
        except sqlite3.IntegrityError:
            logger.warning("User already exists (integrity error): %s", muuid)
            return False
        except sqlite3.Error as e:
            logger.exception("Failed to add user: %s", e)
            return False


user_db = UserDatabase(DB_PATH)


# RabbitMQ Connection & Setup


def get_rabbitmq_connection() -> pika.BlockingConnection:
    """Create a RabbitMQ connection with retry and timeout settings."""
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
    """Initialize RabbitMQ exchanges and topology."""
    logger.info("Initializing RabbitMQ exchanges")

    connection = None
    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()

        # Declare check-in exchange
        logger.info("Declaring exchange: %s", CHECKIN_EXCHANGE)
        channel.exchange_declare(
            exchange=CHECKIN_EXCHANGE,
            exchange_type="topic",
            durable=True,
        )

        # Declare heartbeat exchange
        logger.info("Declaring exchange: %s", HEARTBEAT_EXCHANGE)
        channel.exchange_declare(
            exchange=HEARTBEAT_EXCHANGE,
            exchange_type="direct",
            durable=True,
        )

        # Declare CRM user confirmed exchange
        logger.info("Declaring exchange: %s", CRM_USER_CONFIRMED_EXCHANGE)
        channel.exchange_declare(
            exchange=CRM_USER_CONFIRMED_EXCHANGE,
            exchange_type="topic",
            durable=True,
        )

        # Declare kassa exchange if in kassa mode
        if KASSA_SIGN_IN_MODE:
            logger.info(
                "Declaring exchange: %s (KASSA mode)", KASSA_AUTHENTICATE_EXCHANGE
            )
            channel.exchange_declare(
                exchange=KASSA_AUTHENTICATE_EXCHANGE,
                exchange_type="direct",
                durable=True,
            )

        logger.info("RabbitMQ setup complete")

    except AMQPConnectionError as e:
        logger.exception("Failed to connect to RabbitMQ: %s", e)
        raise
    except Exception as e:
        logger.exception("Failed to initialize RabbitMQ: %s", e)
        raise
    finally:
        if connection:
            try:
                connection.close()
            except Exception as e:
                logger.warning("Error closing RabbitMQ connection: %s", e)


def _declare_required_exchanges(
    channel: pika.adapters.blocking_connection.BlockingChannel,
):
    """Declare all required exchanges on the given channel."""
    channel.exchange_declare(
        exchange=CHECKIN_EXCHANGE,
        exchange_type="topic",
        durable=True,
    )

    if KASSA_SIGN_IN_MODE:
        channel.exchange_declare(
            exchange=KASSA_AUTHENTICATE_EXCHANGE,
            exchange_type="direct",
            durable=True,
        )

    channel.exchange_declare(
        exchange=HEARTBEAT_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )


# XSD Schema Loading


def load_xsd_schema(path: str) -> etree.XMLSchema:
    """Load and parse an XSD schema from a file."""
    logger.info("Loading XSD schema from: %s", path)

    try:
        with open(path, "rb") as f:
            xsd_doc = etree.parse(f)

        schema = etree.XMLSchema(xsd_doc)
        logger.info("Successfully loaded schema: %s", path)
        return schema

    except FileNotFoundError:
        logger.exception("XSD schema file not found: %s", path)
        raise
    except etree.XMLSyntaxError as e:
        logger.exception("XSD schema syntax error: %s", e)
        raise
    except Exception as e:
        logger.exception("Failed to load XSD schema: %s", e)
        raise


# Publishing with Retry Logic


def _publish_with_retry(
    exchange: str,
    routing_key: str,
    xml_bytes: bytes,
    kind: str,
):
    """Publish a message to RabbitMQ with exponential backoff retry."""
    last_error = None

    for attempt in range(1, PUBLISH_RETRIES + 1):
        connection = None
        try:
            connection = get_rabbitmq_connection()
            channel = connection.channel()

            # Re-declare exchanges in case broker was restarted
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

            logger.info("%s message published successfully", kind)
            return

        except (AMQPError, OSError) as e:
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
                    logger.warning("Error closing RabbitMQ connection: %s", e)

    logger.exception(
        "Failed to publish %s after %d attempts: %s",
        kind,
        PUBLISH_RETRIES,
        last_error,
    )
    raise last_error


# Check-in XML Building, Validation, and Publishing


def build_checkin_xml(data: dict) -> bytes:
    """Build and serialize a check-in XML message."""
    logger.info("Building check-in XML for id=%s", data.get("id"))

    root = etree.Element("CheckIn")

    id_el = etree.SubElement(root, "id")
    id_el.text = str(data["id"])

    ts_el = etree.SubElement(root, "timestamp")
    # Ensure timestamps are timezone-aware
    ts_el.text = data.get("timestamp") or datetime.now().astimezone().isoformat()

    xml = etree.tostring(
        root,
        xml_declaration=True,
        encoding="UTF-8",
    )

    logger.info("Check-in XML built successfully")
    return xml


def validate_checkin_xml(xml_bytes: bytes) -> bool:
    """Validate check-in XML against XSD schema."""
    logger.info("Validating check-in XML")

    try:
        doc = etree.fromstring(xml_bytes)
        valid = CHECKIN_XSD_SCHEMA.validate(doc)

        if valid:
            logger.info("Check-in XML validation successful")
        else:
            logger.error("Check-in XML validation failed")

        return valid

    except Exception as e:
        logger.exception("Exception during check-in XML validation: %s", e)
        return False


def publish_checkin(xml_bytes: bytes):
    """Publish a check-in message to RabbitMQ."""
    routing_key = (
        CHECKIN_KASSA_ROUTING_KEY
        if KASSA_SIGN_IN_MODE
        else CHECKIN_CONTROLROOM_ROUTING_KEY
    )

    logger.info(
        "Publishing check-in message exchange=%s routing_key=%s",
        CHECKIN_EXCHANGE,
        routing_key,
    )

    _publish_with_retry(
        exchange=CHECKIN_EXCHANGE,
        routing_key=routing_key,
        xml_bytes=xml_bytes,
        kind="check-in",
    )


def publish_kassa_authenticate(xml_bytes: bytes):
    """Publish a Kassa authentication message to RabbitMQ."""
    logger.info(
        "Publishing Kassa authentication message exchange=%s routing_key=%s",
        KASSA_AUTHENTICATE_EXCHANGE,
        KASSA_AUTHENTICATE_ROUTING_KEY,
    )

    _publish_with_retry(
        exchange=KASSA_AUTHENTICATE_EXCHANGE,
        routing_key=KASSA_AUTHENTICATE_ROUTING_KEY,
        xml_bytes=xml_bytes,
        kind="kassa-authenticate",
    )


# Heartbeat XML Building, Validation, and Publishing


def build_heartbeat_xml(data: dict) -> bytes:
    """Build and serialize a heartbeat XML message."""
    logger.info("Building heartbeat XML for service=%s", data.get("serviceId"))

    root = etree.Element("Heartbeat")

    serv_id_el = etree.SubElement(root, "serviceId")
    serv_id_el.text = str(data["serviceId"])

    ts_el = etree.SubElement(root, "timestamp")
    ts_el.text = data.get("timestamp") or datetime.now().astimezone().isoformat()

    xml = etree.tostring(
        root,
        xml_declaration=True,
        encoding="UTF-8",
    )

    logger.info("Heartbeat XML built successfully")
    return xml


def validate_heartbeat_xml(xml_bytes: bytes) -> bool:
    """Validate heartbeat XML against XSD schema."""
    logger.info("Validating heartbeat XML")

    try:
        doc = etree.fromstring(xml_bytes)
        valid = HEARTBEAT_XSD_SCHEMA.validate(doc)

        if valid:
            logger.info("Heartbeat XML validation successful")
        else:
            logger.error("Heartbeat XML validation failed")

        return valid

    except Exception as e:
        logger.exception("Exception during heartbeat XML validation: %s", e)
        return False


def publish_heartbeat(xml_bytes: bytes):
    """Publish a heartbeat message to RabbitMQ."""
    logger.info(
        "Publishing heartbeat exchange=%s routing_key=%s",
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
    """Build, validate, and publish a heartbeat message."""
    logger.info("Sending heartbeat")

    xml_bytes = build_heartbeat_xml({"serviceId": SERVICE_TAG})

    if not validate_heartbeat_xml(xml_bytes):
        logger.error("Failed to validate heartbeat")
        return

    try:
        publish_heartbeat(xml_bytes)
        logger.info("Heartbeat sent successfully")
    except Exception as e:
        logger.exception("Failed to send heartbeat: %s", e)


# CRM User Consumer


def consume_crm_users():
    """Consume CRM user confirmation messages and store in database."""
    logger.info("Starting CRM user consumer")

    connection = None
    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()

        # Declare exchange and queue
        channel.exchange_declare(
            exchange=CRM_USER_CONFIRMED_EXCHANGE,
            exchange_type="topic",
            durable=True,
        )

        channel.queue_declare(
            queue=CRM_USER_CONFIRMED_QUEUE,
            durable=True,
        )

        channel.queue_bind(
            exchange=CRM_USER_CONFIRMED_EXCHANGE,
            queue=CRM_USER_CONFIRMED_QUEUE,
            routing_key=CRM_USER_CONFIRMED_ROUTING_KEY,
        )

        channel.basic_qos(prefetch_count=1)

        def on_message(ch, method, properties, body):
            """Handle incoming CRM user confirmation."""
            logger.info("Received CRM user confirmation message")

            if properties.content_type != "application/xml":
                logger.error("Invalid content type: %s", properties.content_type)
                ch.basic_nack(delivery_tag=method.delivery_tag)
                return

            try:
                root = etree.fromstring(body)
                muuid = root.findtext("muuid")

                if not muuid:
                    logger.error("Missing muuid in CRM message")
                    ch.basic_nack(delivery_tag=method.delivery_tag)
                    return

                if user_db.user_exists(muuid):
                    logger.info("User already exists in database: %s", muuid)
                else:
                    user_db.add_user(muuid)

                ch.basic_ack(delivery_tag=method.delivery_tag)

            except etree.XMLSyntaxError as e:
                logger.exception("Failed to parse XML: %s", e)
                ch.basic_nack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logger.exception("Error processing CRM user message: %s", e)
                ch.basic_nack(delivery_tag=method.delivery_tag)

        channel.basic_consume(
            queue=CRM_USER_CONFIRMED_QUEUE,
            on_message_callback=on_message,
        )

        logger.info(
            "CRM user consumer listening on queue: %s", CRM_USER_CONFIRMED_QUEUE
        )
        channel.start_consuming()

    except AMQPConnectionError as e:
        logger.exception("Failed to connect for consumer: %s", e)
    except Exception as e:
        logger.exception("Consumer error: %s", e)
    finally:
        if connection:
            try:
                connection.close()
            except Exception as e:
                logger.warning("Error closing consumer connection: %s", e)


# HTTP Request Handler


class Handler(BaseHTTPRequestHandler):
    """HTTP request handler for check-in submissions."""

    def do_POST(self):
        """Handle POST requests containing check-in data."""
        logger.info("Received POST request path=%s", self.path)

        try:
            length = int(self.headers.get("Content-Length", 0))

            if length == 0:
                logger.error("Empty request body")
                self._respond(400, b"Empty request body")
                return

            logger.info("Reading request body length=%d", length)
            body = self.rfile.read(length)

            logger.info("Parsing JSON payload")
            data = json.loads(body)

            logger.info("Request payload parsed successfully")

            xml_bytes = build_checkin_xml(data)

            if not validate_checkin_xml(xml_bytes):
                logger.error("Invalid check-in XML")
                self._respond(400, b"Invalid XML schema")
                return

            publish_checkin(xml_bytes)

            logger.info("Request processed successfully")
            self._respond(200, b"OK")

        except json.JSONDecodeError as e:
            logger.exception("Failed to decode JSON: %s", e)
            self._respond(400, b"Invalid JSON")

        except Exception as e:
            logger.exception("Unexpected server error: %s", e)
            self._respond(500, b"Internal Server Error")

    def _respond(self, code: int, message: bytes):
        """Send HTTP response."""
        logger.info("Responding with status=%d", code)

        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(message)

    def log_message(self, format, *args):
        """Suppress default HTTP server logging."""
        return


# Server & Main


def start_http_server():
    """Start the HTTP server."""
    logger.info("Starting HTTP server host=%s port=%d", HOST, PORT)

    server = HTTPServer((HOST, PORT), Handler)
    logger.info("HTTP server listening")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("HTTP server shutting down")
        server.shutdown()


def main():
    """Main entry point."""
    global CHECKIN_XSD_SCHEMA
    global HEARTBEAT_XSD_SCHEMA

    logger.info("Starting Badge Scanner service")

    try:
        # Load XSD schemas
        CHECKIN_XSD_SCHEMA = load_xsd_schema(CHECKIN_XSD_PATH)
        HEARTBEAT_XSD_SCHEMA = load_xsd_schema(HEARTBEAT_XSD_PATH)

        # Initialize RabbitMQ
        setup_rabbitmq()

        logger.info("Initial setup complete")

        # Start CRM user consumer in background thread
        consumer_thread = Thread(
            target=consume_crm_users,
            daemon=True,
            name="crm-consumer",
        )
        consumer_thread.start()
        logger.info("CRM consumer thread started")

        # Start HTTP server (blocking)
        start_http_server()

        return 0

    except Exception as e:
        logger.exception("Fatal error during startup: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
