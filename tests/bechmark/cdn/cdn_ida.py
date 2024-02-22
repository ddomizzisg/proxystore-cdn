from __future__ import annotations

from proxystore.connectors.cdn import CDNConnector
from utils import randbytes

import time

def test_set(
    connector: CDNConnector,
    payload_size_bytes: int,
    repeat: int = 1,
    number_of_chunks: int = 1,
    required_chunks: int = 1,
    workers: int = 1
) -> list[float]:
    times_ms: list[float] = []
    
    disperse = "IDA" if number_of_chunks > 1 else "SINGLE"

    for i in range(repeat):
        data = randbytes(payload_size_bytes)
        start = time.perf_counter_ns()
        key,time_metrics = connector.put(
                data, 
                number_of_chunks=number_of_chunks, 
                required_chunks=required_chunks, 
                workers=workers
            )
        end = time.perf_counter_ns()
        time_metrics["total_time"] = (end - start) / 1e6
        times_ms.append(time_metrics)

        # Evict key immediately to keep memory usage low
        del data
        connector.evict(key)

    return times_ms

def test_get(
    connector: CDNConnector,
    payload_size_bytes: int,
    repeat: int = 1,
) -> list[float]:
    times_ms: list[float] = []

    data = randbytes(payload_size_bytes)
    key = connector.put(data)
    
    for i in range(repeat):
        start = time.perf_counter_ns()
        data_ = connector.get(key)
        end = time.perf_counter_ns()
        times_ms.append((end - start) / 1e6)
        del data_
    # Evict key immediately to keep memory usage low
    connector.evict(key)
    
    return times_ms

