#!/usr/bin/env python3
"""Generic redis-py smoke against Ferredis (no Galaxy dependency).

RESP3 : `REDIS_PROTOCOL=3` (défaut 3) ; forcer 2 via REDIS_PROTOCOL=2 si besoin.
"""
from __future__ import annotations

import os
import sys

import redis

URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6380/0")
PROTOCOL = int(os.environ.get("REDIS_PROTOCOL", "3"))


def main() -> int:
    r = redis.Redis.from_url(URL, decode_responses=True, protocol=PROTOCOL)
    assert r.ping() is True
    r.flushdb()
    assert r.set("k", "v", ex=60) is True
    assert r.get("k") == "v"
    assert r.set("lock", "1", nx=True, ex=30) is True
    assert r.set("lock", "2", nx=True) is None
    assert r.incr("c") == 1
    assert r.lpush("q", "a", "b") == 2
    assert r.brpop("q", timeout=1) is not None
    p = r.pubsub()
    p.subscribe("ch")
    # consume subscribe ack
    assert p.get_message(timeout=1) is not None
    assert r.publish("ch", "hi") >= 1
    msg = p.get_message(timeout=1)
    # may need skip subscribe leftovers
    for _ in range(5):
        if msg and msg.get("type") == "message":
            break
        msg = p.get_message(timeout=1)
    assert msg and msg.get("data") == "hi", msg
    cur, keys = r.scan(0, match="*", count=50)
    assert isinstance(keys, list)
    print("OK", URL, "protocol=", PROTOCOL, "scan_cursor=", cur, "keys=", keys)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print("FAIL:", exc, file=sys.stderr)
        raise SystemExit(1)
