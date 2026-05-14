import json
import os
import sys
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

import pika
from lxml import etree

# Config
HOST = os.getenv("INTERNAL_HOST", "0.0.0.0")
PORT = int(os.getenv("EXTERNAL_PORT", "8080"))

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")


# TODO(nasr): throw this in an iteratable array or some config  block later or a map could also be cool
CHECKIN_EXCHANGE = "users.checkin.topic"
CHECKIN_ROUTING_KEY = "routing.user.checkin"

HEARTBEAT_EXCHANGE = "heartbeat.direct"
HEARTBEAT_KEY = "routing.heartbeat"

CHECKIN_XSD_PATH = "./xsd/checkin.xsd"
HEARTBEAT_XSD_PATH = "./xsd/heartbeat.xsd"

channel = None
CHECKIN_XSD_SCHEMA = None
HEARTBEAT_XSD_SCHEMA = None


#########################################################################
# Helper functions
# XSD loading
def load_xsd_schema(path: str) -> etree.XMLSchema:
    with open(path, "rb") as f:
        xsd_doc = etree.parse(f)
    return etree.XMLSchema(xsd_doc)


def setup_rabbitmq():
    global channel

    credentials = pika.PlainCredentials(username=RABBITMQ_USER, password=RABBITMQ_PASS)

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    )

    channel = connection.channel()

    channel.exchange_declare(
        exchange=CHECKIN_EXCHANGE,
        exchange_type="topic",
        durable=True,
    )


#########################################################################


####################################################
# checkin building, validation, and publishing
def build_checkin_xml(data: dict) -> bytes:
    root = etree.Element("CheckIn")

    id_el = etree.SubElement(root, "id")
    id_el.text = str(data["id"])

    ts_el = etree.SubElement(root, "timestamp")
    ts_el.text = data.get("timestamp") or datetime.now().isoformat()

    return etree.tostring(root, xml_declaration=True, encoding="UTF-8")


def validate_checkin_xml(xml_bytes: bytes) -> bool:
    try:
        doc = etree.fromstring(xml_bytes)
        return CHECKIN_XSD_SCHEMA.validate(doc)
    except Exception:
        return False


def publish_checkin(xml_bytes: bytes):
    channel.basic_publish(
        exchange=CHECKIN_EXCHANGE,
        routing_key=CHECKIN_ROUTING_KEY,
        body=xml_bytes,
    )


#####################################################

####################################################
# heartbeat building, validation and publishing


def build_heartbeat_xml(data: dict) -> bytes:

    root = etree.Element("Heartbeat")

    serv_id_el = e.tree.SubElement(root, "serviceId")
    serv_id_el.ttext = str(data["serviceId"])

    ts_el = etree.SubElement(root, "timestamp")
    ts_el.text = data.get("timestamp") or datetime.now().isoformat()

    return etree.tostring(root, xml_declaration=True, encoding="UTF-8")


def validate_heaartbeat_xml(xml_bytes: bytes) -> bool:
    try:
        doc = etree.fromstring(xml_bytes)
        return HEARTBEAT_XSD_SCHEMA.validate(doc)
    except Exception:
        return False


def publish_heartbeat(xml_bytes: bytes):
    channel.basic_publish(
        exchange=HEARTBEAT_EXCHANGE, routing_key=HEARTBEAT_ROUTING_KEY, body=xml_bytes
    )


def send_heartbeat():
    data = build_heartbeat_xml("BADGE_SCANNER")
    validated = validate_heartbeat_xml(data)
    if validated == false:
        logger.error("failed to validate the heartbeat")
        return
    publih_heartbeat(data)


#####################################################
# HTTP stuff


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)

            data = json.loads(body)

            xml_bytes = build_checkin_xml(data)

            if not validate_checkin_xml(xml_bytes):
                self._respond(400, b"Invalid XML schema")
                return

            publish_checkin(xml_bytes)
            self._respond(200, b"OK: TOPPIE FLOPPIE")

        except Exception:
            self._respond(500, b"ERROR: OEI KAPOT JONGE")

    def _respond(self, code: int, message: bytes):
        self.send_response(code)
        self.end_headers()
        self.wfile.write(message)

    def log_message(self, format, *args):
        return


# Server stuff
def listen():
    server = HTTPServer((HOST, PORT), Handler)
    server.serve_forever()


#####################################################


def main():
    global CHECKIN_XSD_SCHEMA

    CHECKIN_XSD_SCHEMA = load_xsd_schema(CHECKIN_XSD_PATH)
    HEARTBEAT_XSD_SCHEMA = load_xsd_schema(HEARTBEAT_XSD_PATH)

    setup_rabbitmq()
    listen()

    return 0


if __name__ == "__main__":
    sys.exit(main())
