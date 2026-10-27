#!/usr/bin/env python3
"""
Galaxy smoke against whatever REDIS_URL points to (Ferredis drop-in).
Does not modify Galaxy sources — uses Django setup from the Galaxy project.
"""
from __future__ import annotations

import os
import sys


def main() -> int:
    galaxy_root = os.environ.get(
        "GALAXY_ROOT",
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "galaxy_reborn")),
    )
    if galaxy_root not in sys.path:
        sys.path.insert(0, galaxy_root)
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "galaxy.settings")

    import django

    django.setup()

    from django.contrib.auth import authenticate
    from django.core.cache import cache
    import redis
    from django.conf import settings

    url = settings.REDIS_URL
    r = redis.Redis.from_url(url)
    assert r.ping(), "Redis PING failed"

    cache.delete_many(["ferredis:smoke", "ferredis:lock", "ferredis:counter"])
    cache.set("ferredis:smoke", {"ok": True}, timeout=60)
    assert cache.get("ferredis:smoke") == {"ok": True}
    assert cache.add("ferredis:lock", "1", timeout=30) is True
    assert cache.add("ferredis:lock", "2", timeout=30) is False
    cache.set("ferredis:counter", 0, timeout=60)
    cache.incr("ferredis:counter")
    assert int(cache.get("ferredis:counter")) == 1

    user = authenticate(
        username=os.environ.get("GALAXY_USER", "admin_test"),
        password=os.environ.get("GALAXY_PASS", "admin123"),
    )
    assert user is not None and user.is_active, "auth failed"

    print("OK galaxy smoke against", url, "user=", user.username)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print("FAIL:", exc, file=sys.stderr)
        raise
