import json

from app import find_brti_value


def test_reads_live_spot_from_raw_cfbenchmarks_frame():
    payload = {
        "index_id": "BRTI",
        "received_at": 1795150000000,
        "data": json.dumps({
            "id": "BRTI",
            "value": "61820.25",
            "timestamp": 1795149999000,
        }),
        "avg_60s_data": {
            "value": "61810.00",
            "window_end_ts_exclusive": 1795150000000,
        },
    }

    assert find_brti_value(payload) == (61820.25, 1795149999000)


def test_uses_official_60_second_average_only_as_fallback():
    payload = {
        "index_id": "BRTI",
        "received_at": 1795150000000,
        "avg_60s_data": {
            "value": "61810.00",
            "window_end_ts_exclusive": 1795150000000,
        },
    }

    assert find_brti_value(payload) == (61810.0, 1795150000000)


def test_reads_spot_from_wrapped_websocket_message():
    payload = {
        "type": "cfbenchmarks_value",
        "msg": {
            "index_id": "BRTI",
            "data": json.dumps({
                "value_usd": "61830.50",
                "source_ts_ms": 1795150000000,
            }),
        },
    }

    assert find_brti_value(payload) == (61830.5, 1795150000000)
