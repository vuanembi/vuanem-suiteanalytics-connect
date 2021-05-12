import os
import subprocess
import re
import ast
from datetime import datetime

import requests

from .utils import assertion, encode_data

RESULTS_REGEX = "{\\'pipelines.*}"


def process_output(out):
    out = out.decode("utf-8")
    res = re.search(RESULTS_REGEX, out).group(0)
    res = ast.literal_eval(res)
    return res

def open_process(port):
    return subprocess.Popen(
        [
            "functions-framework",
            "--target=main",
            f"--port={port}",
        ],
        cwd=os.getcwd(),
        stdout=subprocess.PIPE,
    )


def test_standard():
    """Test the scripts for default/auto mode"""

    port = 8080
    process = open_process(port)

    data = {"table": "CLASSES"}
    message = encode_data(data)
    with requests.post(f"http://localhost:{port}", json=message) as r:
        res = r.json()
    process.kill()
    process.wait()
    results = res.get('results')
    assertion(results)


def test_incremental_auto():
    """Test the scripts for default/auto mode"""

    port = 8082
    process = open_process(port)

    data = {"table": "TRANSACTIONS"}
    message = encode_data(data)
    with requests.post(f"http://localhost:{port}", json=message) as r:
        res = r.json()
    process.kill()
    process.wait()
    results = res.get('results')
    assertion(results)


def test_incremental_manual():
    """Test the scripts for default/auto mode"""

    port = 8083
    process = open_process(port)

    data = {
        "table": "TRANSACTIONS",
        "start": datetime(2018, 6, 30).strftime("%Y-%m-%d"),
        "end": datetime(2018, 7, 10).strftime("%Y-%m-%d"),
    }
    message = encode_data(data)
    with requests.post(f"http://localhost:{port}", json=message) as r:
        res = r.json()
    process.kill()
    process.wait()
    results = res.get('results')
    assertion(results)
