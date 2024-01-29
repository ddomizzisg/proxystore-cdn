from __future__ import annotations

from proxystore.store import Store
from proxystore.connectors.cdn import CDNConnector
from utils import randbytes

import time

def test_set(
    store: Store,
    payload_size_bytes: int,
    repeat: int = 1,
) -> list[float]:
    times_ms: list[float] = []

    for i in range(repeat):
        data = randbytes(payload_size_bytes)
        start = time.perf_counter_ns()
        key = store.put(data)
        end = time.perf_counter_ns()
        times_ms.append((end - start) / 1e6)

        # Evict key immediately to keep memory usage low
        del data
        store.evict(key)

    return times_ms

def test_get(
    store: Store,
    payload_size_bytes: int,
    repeat: int = 1,
) -> list[float]:
    times_ms: list[float] = []

    data = randbytes(payload_size_bytes)
    key = store.put(data)
    
    for i in range(repeat):
        start = time.perf_counter_ns()
        data_ = store.get(key)
        end = time.perf_counter_ns()
        times_ms.append((end - start) / 1e6)
        del data_

    # Evict key immediately to keep memory usage low
    store.evict(key)

    return times_ms

