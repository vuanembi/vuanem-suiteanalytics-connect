def assertion(res):
    assert res["num_processed"] > 0
    assert res["output_rows"] > 0
    assert res["num_processed"] == res["output_rows"]
    assert res["errors"] is None
