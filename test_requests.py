import requests


def test_requests():
    with requests.get("http://localhost:8080") as r:
        response = r.json()
    for i in response.get("results"):
        assert i["num_processed"] > 0
        assert i["output_rows"] > 0
        assert i["errors"] is not None
