from __future__ import annotations

import pandas as pd
import cdn_ida as ops_ida
import os
import sys
import statistics
from typing import Any
from typing import Literal
from typing import NamedTuple
from typing import Sequence
import logging

from pscsv import CSVLogger
from pslogging import init_logging
from pslogging import TESTING_LOG_LEVEL

from proxystore.connectors.cdn import CDNConnector

BACKEND_TYPE = Literal['ENDPOINT', 'CDN']
OP_TYPE = Literal['EVICT', 'EXISTS', 'GET', 'SET']

class RunStats(NamedTuple):
    """Stats for a given run configuration."""

    backend: BACKEND_TYPE
    op: OP_TYPE
    payload_size_bytes: int | None
    repeat: int
    total_time_ms: float
    avg_time_ms: float
    min_time_ms: float
    clients: int | None
    workers: int | None
    n: int | None
    k: int | None
    max_time_ms: float
    stdev_time_ms: float
    metadata_time: float | None
    data_upload_time: float | None
    throughput: float | None
    throughput_upload: float | None
    chunking_time: float | None

def create_random_file(size_mb):
    size_bytes = int(size_mb * 1024 * 1024)  # Convert MB to bytes
    return os.urandom(size_bytes)

logger = logging.getLogger('remote-ops')

trace = "all_traces/trace_drex_sc_0.999_8.csv"

df = pd.read_csv(trace)

conn = CDNConnector(catalog="test", user_token="d5439067ee33a0e695be07a79f8a3b07c35a98f933cb9321a37d38f92f5581c9", gateway="192.5.86.191:8095")

csv_logger = CSVLogger(sys.argv[1], RunStats)

for i,row in enumerate(df.iterrows()):
    if i > 2173:
        data_size = 1 #int(row[1]['data_size']  * 1024 * 1024)
        #print("Size",data_size)
        number_of_chunks = row[1][' N']
        required_chunks = row[1][' K']
        chosen_nodes = row[1][' chosen_nodes'].strip().split(" ")
        print(number_of_chunks, required_chunks, chosen_nodes)
        with open(f"/media/storage1/objects/{i}.obj", "rb") as f:
            times_ms = ops_ida.test_set(
                conn, data_size, repeat=4, 
                number_of_chunks=number_of_chunks, 
                required_chunks=required_chunks, workers=1, 
                nodes=chosen_nodes, data=f.read())
        
        if len(times_ms) > 0:
            total_time = sum([x["total_time"] for x in times_ms])
            #print("entro", total_time, len(times_ms))
            avg_total_time = total_time / len(times_ms)
            avg_metadata_time = sum([x["metadata_time"]
                                    for x in times_ms]) / len(times_ms)
            avg_data_upload_time = sum([x["upload_time"]
                                        for x in times_ms]) / len(times_ms)
            avg_data_chunking_time = sum([x["chunking_time"]
                                        for x in times_ms]) / len(times_ms)
            payload_mb = data_size / 1e6
            throughput = payload_mb / avg_total_time
            throughput_upload = payload_mb / (total_time / 1000)

            rs = RunStats(
                backend='CDN',
                op='SET',
                payload_size_bytes=data_size,
                clients=1,
                repeat=10,
                n=number_of_chunks,
                k=required_chunks,
                workers=1,
                total_time_ms=total_time,
                avg_time_ms=avg_total_time,
                min_time_ms=min([x["total_time"] for x in times_ms]),
                max_time_ms=max([x["total_time"] for x in times_ms]),
                stdev_time_ms=(
                    statistics.stdev([x["total_time"] for x in times_ms]) if len(
                        times_ms) > 1 else 0.0
                ),
                throughput=throughput,
                metadata_time=avg_metadata_time,
                data_upload_time=avg_data_upload_time,
                throughput_upload=throughput_upload,
                chunking_time=avg_data_chunking_time
            )
            
            logger.log(TESTING_LOG_LEVEL,rs)
            csv_logger.log(rs)
            
            #break
        #del data