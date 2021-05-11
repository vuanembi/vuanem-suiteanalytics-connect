import os
import subprocess
import base64
import json
import re
import ast
import time
from datetime import datetime

import requests

from .assertions import assertion

RESULTS_REGEX = "{\\'pipelines.*}"


def process_output(out):
    out = out.decode("utf-8")
    res = re.search(RESULTS_REGEX, out).group(0)
    res = ast.literal_eval(res)
    return res


def test_standard():
    """Test the scripts for default/auto mode"""

    port = 8080
    process = subprocess.Popen(
        [
            "functions-framework",
            "--target=main",
            "--signature-type=event",
            f"--port={port}",
        ],
        cwd=os.getcwd(),
        stdout=subprocess.PIPE,
    )

    message = {"table": "CLASSES"}
    message_json = json.dumps(message)
    message_encoded = base64.b64encode(message_json.encode("utf-8")).decode("utf-8")
    event = {"data": {"data": message_encoded}}
    with requests.post(f"http://localhost:{port}", json=event) as r:
        assert r.status_code == 200
    process.kill()
    process.wait()
    out, err = process.communicate()
    res = process_output(out)
    res = res.get('results')
    assertion(res)


def test_incremental_auto():
    """Test the scripts for default/auto mode"""

    port = 8082
    process = subprocess.Popen(
        [
            "functions-framework",
            "--target=main",
            "--signature-type=event",
            f"--port={port}",
        ],
        cwd=os.path.dirname(__file__),
        stdout=subprocess.PIPE,
    )

    message = {"table": "TRANSACTIONS"}
    message_json = json.dumps(message)
    message_encoded = base64.b64encode(message_json.encode("utf-8")).decode("utf-8")
    event = {"data": {"data": message_encoded}}
    with requests.post(f"http://localhost:{port}", json=event) as r:
        assert r.status_code == 200
    process.kill()
    process.wait()
    out, err = process.communicate()
    res = process_output(out)
    res = res.get('results')
    assertion(res)


def test_incremental_manual():
    """Test the scripts for default/auto mode"""

    port = 8083
    process = subprocess.Popen(
        [
            "functions-framework",
            "--target=main",
            "--signature-type=event",
            f"--port={port}",
        ],
        cwd=os.getcwd(),
        stdout=subprocess.PIPE,
    )

    time.sleep(5)

    message = {
        "table": "TRANSACTIONS",
        "start": datetime(2018, 6, 30).strftime("%Y-%m-%d"),
        "end": datetime(2018, 7, 10).strftime("%Y-%m-%d"),
    }
    message_json = json.dumps(message)
    message_encoded = base64.b64encode(message_json.encode("utf-8")).decode("utf-8")
    event = {"data": {"data": message_encoded}}
    with requests.post(f"http://localhost:{port}", json=event) as r:
        assert r.status_code == 200
    process.kill()
    process.wait()
    out, err = process.communicate()
    res = process_output(out)
    res = res.get('results')
    assertion(res)
