import pytest
from lxml import etree

import server.main as main

@pytest.fixture(scope="module")
def schemas():
    checkin_schema = etree.XMLSchema(etree.parse("server/xsd/checkin.xsd"))
    heartbeat_schema = etree.XMLSchema(etree.parse("server/xsd/heartbeat.xsd"))
    return checkin_schema, heartbeat_schema


def test_xml_generation():
    muuid = "test-uuid-456"
    checkin_xml = main.build_checkin_xml(muuid)

    # Parse and check tag and content
    root = etree.fromstring(checkin_xml)
    assert root.tag == "CheckIn"
    assert root.findtext("id") == muuid
    assert root.findtext("timestamp") is not None

    heartbeat_xml = main.build_heartbeat_xml()
    root_hb = etree.fromstring(heartbeat_xml)
    assert root_hb.tag == "Heartbeat"
    assert root_hb.findtext("serviceId") == "iot-badge-scanner"
    assert root_hb.findtext("timestamp") is not None


def test_xml_validation(schemas):
    checkin_schema, heartbeat_schema = schemas

    # Valid checkin
    valid_checkin = main.build_checkin_xml("uuid-1")
    assert main._validate(checkin_schema, valid_checkin) is True

    # Invalid checkin (missing timestamp / incorrect elements)
    invalid_checkin = b"<CheckIn><id>uuid-1</id></CheckIn>"
    assert main._validate(checkin_schema, invalid_checkin) is False

    # Valid heartbeat
    valid_hb = main.build_heartbeat_xml()
    assert main._validate(heartbeat_schema, valid_hb) is True

    # Invalid heartbeat XML syntax
    assert main._validate(heartbeat_schema, b"invalid-xml-syntax") is False

