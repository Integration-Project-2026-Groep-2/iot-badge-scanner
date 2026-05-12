import os
import sys
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime

import pika
from lxml import etree


# Config
HOST = os.getenv("INTERNAL_HOST", "0.0.0.0")
PORT = int(os.getenv("EXTERNAL_PORT", "8080"))

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")

# TODO(nasr): throw this in an iteratable array or some config  block later or a map could also be cool
CHECKIN_EXCHANGE = "users.checkin.topic"
CHECKIN_ROUTING_KEY = "routing.user.checkin"

XSD_PATH = "./xsd/checkin.xsd"


channel = None
CHECKIN_XSD_SCHEMA = None


# XSD loading
def load_xsd_schema(path: str) -> etree.XMLSchema:
    with open(path, "rb") as f:
        xsd_doc = etree.parse(f)
    return etree.XMLSchema(xsd_doc)


def setup_rabbitmq():
    global channel

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )

    channel = connection.channel()

    channel.exchange_declare(
        exchange=CHECKIN_EXCHANGE,
        exchange_type="topic",
        durable=True,
    )


def build_xml(data: dict) -> bytes:
    root = etree.Element("CheckIn")

    id_el = etree.SubElement(root, "id")
    id_el.text = str(data["id"])

    ts_el = etree.SubElement(root, "timestamp")
    ts_el.text = data.get("timestamp") or datetime.utcnow().isoformat()

    return etree.tostring(root, xml_declaration=True, encoding="UTF-8")


def validate_xml(xml_bytes: bytes) -> bool:
    try:
        doc = etree.fromstring(xml_bytes)
        return CHECKIN_XSD_SCHEMA.validate(doc)
    except Exception:
        return False


def publish(xml_bytes: bytes):
    channel.basic_publish(
        exchange=CHECKIN_EXCHANGE,
        routing_key=CHECKIN_ROUTING_KEY,
        body=xml_bytes,
    )


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)

            data = json.loads(body)

            xml_bytes = build_xml(data)

            if not validate_xml(xml_bytes):
                self._respond(400, b"Invalid XML schema")
                return

            publish(xml_bytes)
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


def main():
    global CHECKIN_XSD_SCHEMA

    CHECKIN_XSD_SCHEMA = load_xsd_schema(XSD_PATH)

    setup_rabbitmq()
    listen()

    return 0


if __name__ == "__main__":
    sys.exit(main())
