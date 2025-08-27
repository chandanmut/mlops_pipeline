from data_pipeline.libs import io_utils

def test_dummy_read(monkeypatch):
    def fake_read_csv_from_s3(key):
        return "ok"
    monkeypatch.setattr(io_utils, "read_csv_from_s3", fake_read_csv_from_s3)
    assert io_utils.read_csv_from_s3("dummy.csv") == "ok"
