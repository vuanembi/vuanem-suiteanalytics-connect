from datetime import datetime

from .utils import process


def test_accounts():
    data = {"data_source": "NetSuite", "table": "ACCOUNTS"}
    process(data)

def test_budget():
    data = {"data_source": "NetSuite", "table": "BUDGET"}
    process(data)

def test_classes():
    data = {"data_source": "NetSuite", "table": "CLASSES"}
    process(data)

def test_customers():
    data = {"data_source": "NetSuite", "table": "CUSTOMERS"}
    process(data)

def test_delivery_person():
    data = {"data_source": "NetSuite", "table": "DELIVERY_PERSON"}
    process(data)

def test_departments():
    data = {"data_source": "NetSuite", "table": "DEPARTMENTS"}
    process(data)

def test_employees():
    data = {"data_source": "NetSuite", "table": "EMPLOYEES"}
    process(data)

def test_items():
    data = {"data_source": "NetSuite", "table": "ITEMS"}
    process(data)

def test_locations():
    data = {"data_source": "NetSuite", "table": "LOCATIONS"}
    process(data)

def test_system_notes_price():
    data = {"data_source": "NetSuite", "table": "SYSTEM_NOTES_PRICE"}
    process(data)

def test_vendors():
    data = {"data_source": "NetSuite", "table": "VENDORS"}
    process(data)
