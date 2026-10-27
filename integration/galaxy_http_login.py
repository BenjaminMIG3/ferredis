#!/usr/bin/env python3
"""HTTP login smoke against a running Galaxy instance (sessions via Redis/Ferredis)."""
from __future__ import annotations

import os
import re
import sys

import requests

BASE = os.environ.get("GALAXY_URL", "http://127.0.0.1:8000")
USER = os.environ.get("GALAXY_USER", "admin_test")
PASS = os.environ.get("GALAXY_PASS", "admin123")


def main() -> int:
    s = requests.Session()
    login_url = f"{BASE.rstrip('/')}/admin/login/"
    r = s.get(login_url, timeout=20)
    r.raise_for_status()
    m = re.search(
        r'name=["\']csrfmiddlewaretoken["\']\s+value=["\']([^"\']+)',
        r.text,
    )
    if not m:
        print("FAIL: csrf token not found", file=sys.stderr)
        return 2
    token = m.group(1)
    r2 = s.post(
        login_url,
        data={
            "username": USER,
            "password": PASS,
            "csrfmiddlewaretoken": token,
            "next": "/admin/",
        },
        headers={"Referer": login_url},
        timeout=20,
        allow_redirects=False,
    )
    loc = r2.headers.get("Location", "")
    print("POST", r2.status_code, "Location", loc, "cookies", list(s.cookies.keys()))
    if r2.status_code not in (302, 303) or "/admin/login" in loc:
        print("FAIL: login rejected", file=sys.stderr)
        return 1
    r3 = s.get(f"{BASE.rstrip('/')}/admin/", timeout=20, allow_redirects=False)
    print("ADMIN", r3.status_code)
    if r3.status_code not in (200, 302):
        return 1
    # Ensure we are not bounced back to login
    if r3.status_code == 302 and "/login" in r3.headers.get("Location", ""):
        print("FAIL: session not persisted", file=sys.stderr)
        return 1
    print("OK http login")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
