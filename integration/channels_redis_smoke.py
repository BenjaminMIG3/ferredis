#!/usr/bin/env python3
"""channels_redis (couche de canaux Django Channels / WebSocket) contre Ferredis.

Reproduit le trafic d'une connexion WebSocket Daphne sans dépendre d'un projet :
new_channel, send/receive (BZPOPMIN + scripts Lua EVAL), groupes, expiration, flush.

Prérequis : pip install channels_redis redis
Note      : protocol=3 par défaut (HELLO 3 / RESP3). Forcer RESP2 via REDIS_PROTOCOL=2
            si une dépendance Channels l'exige.
Usage     : REDIS_URL=redis://127.0.0.1:6379 python integration/channels_redis_smoke.py
"""
from __future__ import annotations

import asyncio
import os
import sys

from channels_redis.core import RedisChannelLayer

URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")
PROTOCOL = int(os.environ.get("REDIS_PROTOCOL", "3"))
TIMEOUT = 5.0


async def scenario(layer: RedisChannelLayer) -> None:
    # 1. send puis receive (message déjà en file).
    channel = await layer.new_channel()
    await layer.send(channel, {"type": "task.update", "id": 1})
    msg = await asyncio.wait_for(layer.receive(channel), TIMEOUT)
    assert msg == {"type": "task.update", "id": 1}, msg

    # 2. receive bloquant réveillé par un send ultérieur (BZPOPMIN bloquant).
    channel = await layer.new_channel()
    waiter = asyncio.create_task(layer.receive(channel))
    await asyncio.sleep(0.3)
    assert not waiter.done(), "receive doit bloquer tant qu'aucun message n'arrive"
    await layer.send(channel, {"type": "ws.ping"})
    assert await asyncio.wait_for(waiter, TIMEOUT) == {"type": "ws.ping"}

    # 3. ordre FIFO sur un même canal.
    channel = await layer.new_channel()
    for i in range(20):
        await layer.send(channel, {"type": "seq", "i": i})
    got = [(await asyncio.wait_for(layer.receive(channel), TIMEOUT))["i"] for _ in range(20)]
    assert got == list(range(20)), got

    # 4. groupes : plusieurs « clients WebSocket » dans le même groupe.
    group = "tasks"
    clients = [await layer.new_channel() for _ in range(3)]
    for c in clients:
        await layer.group_add(group, c)
    await layer.group_send(group, {"type": "broadcast", "n": 42})
    for c in clients:
        assert await asyncio.wait_for(layer.receive(c), TIMEOUT) == {"type": "broadcast", "n": 42}

    # 5. group_discard : le client retiré ne reçoit plus rien.
    await layer.group_discard(group, clients[0])
    await layer.group_send(group, {"type": "broadcast", "n": 43})
    for c in clients[1:]:
        assert (await asyncio.wait_for(layer.receive(c), TIMEOUT))["n"] == 43
    try:
        await asyncio.wait_for(layer.receive(clients[0]), 0.5)
    except asyncio.TimeoutError:
        pass
    else:
        raise AssertionError("le canal retiré du groupe a reçu un message")

    # 6. plusieurs consommateurs concurrents, chacun sur son canal.
    channels = [await layer.new_channel() for _ in range(10)]
    tasks = [asyncio.create_task(layer.receive(c)) for c in channels]
    await asyncio.sleep(0.2)
    for i, c in enumerate(channels):
        await layer.send(c, {"type": "fanout", "i": i})
    results = await asyncio.wait_for(asyncio.gather(*tasks), TIMEOUT)
    assert [m["i"] for m in results] == list(range(10)), results

    await layer.flush()


async def main() -> int:
    layer = RedisChannelLayer(
        hosts=[{"address": URL, "protocol": PROTOCOL}],
        prefix="ferredis-smoke",
        capacity=100,
    )
    try:
        await scenario(layer)
    finally:
        await layer.close_pools()
    print("OK channels_redis ->", URL, "protocol=", PROTOCOL)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except Exception as exc:  # noqa: BLE001 - affichage lisible pour la CI
        print("ECHEC channels_redis :", repr(exc), file=sys.stderr)
        sys.exit(1)
