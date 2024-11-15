from __future__ import annotations

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

import pyarrow as pa
import pyarrow.fs


BACKEND_TYPE = Literal['ENDPOINT', 'CDN', 'HDFS']
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
    

logger = logging.getLogger('remote-ops')

trace = "all_traces/trace_drex_sc_0.999_8.csv"

# Connect to the HDFS namenode
#fs = hdfs.connect('129.114.109.86', port=9000)

hdfs = pa.fs.HadoopFileSystem(host='129.114.109.86',
    kerb_ticket='/tmp/krb5cc_foo_bar',
    port=9000,
    extra_conf=None
)
