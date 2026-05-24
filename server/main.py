import json
import os
import sqlite3
import sys
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Thread

import pika
from lxml import etree
from pika.exceptions import AMQPError

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from shared.logger import get_logger

logger = get_logger(__name__)


SERVICE_TAG  = "iot-badge-scanner"
HOST         = os.getenv("INTERNAL_HOST", "0.0.0.0")
PORT         = int(os.getenv("EXTERNAL_PORT", "8080"))
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "1"))

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")

PUBLISH_RETRIES     = int(os.getenv("RABBITMQ_PUBLISH_RETRIES", "3"))
PUBLISH_RETRY_DELAY = float(os.getenv("RABBITMQ_PUBLISH_RETRY_DELAY_SECONDS", "0.5"))

CHECKIN_EXCHANGE   = "user.checkin.topic"
CHECKIN_ROUTING    = "routing.controlroom.user.checkin"
HEARTBEAT_EXCHANGE = "heartbeat.direct"
HEARTBEAT_ROUTING  = "routing.heartbeat"
CRM_EXCHANGE       = "contact.topic"
CRM_QUEUE          = "badgescanner.user.confirmed"
CRM_ROUTING        = "crm.user.confirmed"

CHECKIN_XSD_PATH   = "./xsd/checkin.xsd"
HEARTBEAT_XSD_PATH = "./xsd/heartbeat.xsd"
DB_PATH            = "users_muuid_table"

CHECKIN_XSD   = None
HEARTBEAT_XSD = None

def _db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.execute("CREATE TABLE IF NOT EXISTS users (muuid TEXT PRIMARY KEY)")
    return con

def user_exists(muuid: str) -> bool:
    with _db() as con:
        return con.execute("SELECT 1 FROM users WHERE muuid=?", (muuid,)).fetchone() is not None

def add_user(muuid: str):
    try:
        with _db() as con:
            con.execute("INSERT INTO users (muuid) VALUES (?)", (muuid,))
    except sqlite3.IntegrityError:
        pass  # already exists

def _connect() -> pika.BlockingConnection:
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS),
        heartbeat=600,
        blocked_connection_timeout=300,
        connection_attempts=3,
        retry_delay=2,
        ))

def _publish(exchange: str, routing_key: str, body: bytes):
    """Publish with linear backoff retry."""
    for attempt in range(1, PUBLISH_RETRIES + 1):
        try:
            con = _connect()
            ch  = con.channel()
            ch.basic_publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=body,
                    properties=pika.BasicProperties(
                        content_type="application/xml",
                        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                        ),
                    )
            con.close()
            return
        except (AMQPError, OSError) as e:
            if attempt == PUBLISH_RETRIES:
                raise
            logger.warning("publish attempt %d/%d failed: %s", attempt, PUBLISH_RETRIES, e)
            time.sleep(PUBLISH_RETRY_DELAY * attempt)

def _now_iso() -> str:
    return datetime.now().astimezone().isoformat()

def _to_xml(root: etree._Element) -> bytes:
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8")

def _validate(schema: etree.XMLSchema, xml_bytes: bytes) -> bool:
    try:
        return schema.validate(etree.fromstring(xml_bytes))
    except etree.XMLSyntaxError:
        return False

def build_checkin_xml(muuid: str) -> bytes:
    root = etree.Element("CheckIn")
    etree.SubElement(root, "id").text       = muuid
    etree.SubElement(root, "timestamp").text = _now_iso()
    return _to_xml(root)

def build_heartbeat_xml() -> bytes:
    root = etree.Element("Heartbeat")
    etree.SubElement(root, "serviceId").text = SERVICE_TAG
    etree.SubElement(root, "timestamp").text  = _now_iso()
    return _to_xml(root)

def send_heartbeat():
    xml = build_heartbeat_xml()
    if not _validate(HEARTBEAT_XSD, xml):
        logger.error("heartbeat XML invalid — skipping publish")
        return
    try:
        _publish(HEARTBEAT_EXCHANGE, HEARTBEAT_ROUTING, xml)
        logger.info("heartbeat sent")
    except Exception as e:
        logger.error("heartbeat publish failed: %s", e)

def heartbeat_loop():
    while True:
        send_heartbeat()
        time.sleep(HEARTBEAT_INTERVAL)

def consume_crm_users():
    """Block-consume CRM confirmations; store muuid in SQLite."""
    con = _connect()
    ch  = con.channel()
    ch.exchange_declare(exchange=CRM_EXCHANGE, exchange_type="topic", durable=True)
    ch.queue_declare(queue=CRM_QUEUE, durable=True)
    ch.queue_bind(exchange=CRM_EXCHANGE, queue=CRM_QUEUE, routing_key=CRM_ROUTING)
    ch.basic_qos(prefetch_count=1)

    def on_message(ch, method, props, body):
        try:
            muuid = etree.fromstring(body).findtext("id")
            if not muuid:
                raise ValueError("missing muuid")
            add_user(muuid)
            logger.info("stored muuid=%s", muuid)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error("crm message error: %s", e)
            ch.basic_nack(delivery_tag=method.delivery_tag)

    ch.basic_consume(queue=CRM_QUEUE, on_message_callback=on_message)
    logger.info("crm consumer ready")
    ch.start_consuming()

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            if not length:
                return self._respond(400, b"empty body")

            data  = json.loads(self.rfile.read(length))
            muuid = data.get("id")
            if not muuid:
                logger.warning("check-in denied: missing id")
                return self._respond(400, b"missing id")

            if not user_db_has(muuid):
                logger.warning("check-in denied: unknown muuid=%s", muuid)
                return self._respond(403, b"unknown muuid")

            xml = build_checkin_xml(muuid)
            if not _validate(CHECKIN_XSD, xml):
                logger.error("check-in rejected: XML validation failed for muuid=%s", muuid)
                return self._respond(400, b"XML validation failed")

            _publish(CHECKIN_EXCHANGE, CHECKIN_ROUTING, xml)
            self._respond(200, b"OK")

        except json.JSONDecodeError:
            self._respond(400, b"invalid JSON")
        except Exception as e:
            logger.error("handler error: %s", e)
            self._respond(500, b"internal error")

    def _respond(self, code: int, body: bytes):
        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass

def user_db_has(muuid: str) -> bool:
    return user_exists(muuid)

def main() -> int:
    global CHECKIN_XSD, HEARTBEAT_XSD

    logger.info("starting %s", SERVICE_TAG)

    CHECKIN_XSD   = etree.XMLSchema(etree.parse(CHECKIN_XSD_PATH))
    HEARTBEAT_XSD = etree.XMLSchema(etree.parse(HEARTBEAT_XSD_PATH))

    # Declare exchanges once at startup
    con = _connect()
    ch  = con.channel()
    ch.exchange_declare(exchange=CHECKIN_EXCHANGE,   exchange_type="topic",  durable=True)
    ch.exchange_declare(exchange=HEARTBEAT_EXCHANGE, exchange_type="direct", durable=True)
    ch.exchange_declare(exchange=CRM_EXCHANGE,       exchange_type="topic",  durable=True)
    con.close()

    for target, name in [
            (consume_crm_users, "crm-consumer"),
            (heartbeat_loop,    "heartbeat"),
            ]:
        Thread(target=target, name=name, daemon=True).start()
        logger.info("%s thread started", name)

    logger.info("HTTP server on %s:%d", HOST, PORT)
    HTTPServer((HOST, PORT), Handler).serve_forever()
    return 0

if __name__ == "__main__":
    sys.exit(main())
