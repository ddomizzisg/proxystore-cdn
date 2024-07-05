from __future__ import annotations

from proxystore.connectors.cdn import CDNConnector
from utils import randbytes
from proxystore.connectors.cdn import CDNKey

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
        print(key, time_metrics)
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

def test_get_files(
    connector: CDNConnector,
    files: list[str],
    repeat: int = 1
) -> list[float]:
    print(connector)
    times_ms: list[float] = []
    payload = 0
    for i,f in enumerate(files):
        #print(f"Processing file {f}")
        #try:
        for j in range(repeat):
            
            start = time.perf_counter_ns()
            key = CDNKey(cdn_key=str(f))
            #print(key)
            data_ = connector.get(key)
            payload += len(data_)
            end = time.perf_counter_ns()
            times_ms.append((end - start) / 1e6)
            del data_
            # Evict key immediately to keep memory usage low
            #connector.evict(key)
        #except Exception as e:
        #    print(f"Error: {e}")

    return times_ms, payload

def test_set_files(
    connector: CDNConnector,
    files: list[str],
    repeat: int = 1,
    number_of_chunks: int = 1,
    required_chunks: int = 1,
    workers: int = 1,
    resiliency: int = 1
) -> list[float]:
    times_ms: list[float] = []
    
    disperse = "IDA" if number_of_chunks > 1 else "SINGLE"
    print("Entro")
    for i,f in enumerate(files):
        try:
            data = open(f, "rb").read()
            print(repeat)
            for j in range(repeat):
                print("Entro2")
                start = time.perf_counter_ns()
                key,time_metrics = connector.put(
                        data, 
                        workers=workers, resiliency=resiliency
                    )
                print("a", key, time_metrics)
                end = time.perf_counter_ns()
                time_metrics["total_time"] = (end - start) / 1e6
                times_ms.append(time_metrics)

                # Evict key immediately to keep memory usage low
                #del data
                #connector.evict(key)
        except Exception as e:
            print(f"Error: {e}")
            #pass
    return times_ms