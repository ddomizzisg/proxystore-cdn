"""Remote Operation Performance Test.

Provides comparisons between remote operations with endpoints
and CDN servers.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import socket
import statistics
import sys
import uuid
from typing import Any
from typing import Literal
from typing import NamedTuple
from typing import Sequence

from proxystore.endpoint.endpoint import Endpoint

import endpoint_ops as endpoint_ops
import cdn_ops as cdn_ops
from psargparse import add_logging_options
from pscsv import CSVLogger
from pslogging import init_logging
from pslogging import TESTING_LOG_LEVEL

from proxystore.store import Store
from proxystore.connectors.cdn import CDNConnector

BACKEND_TYPE = Literal['ENDPOINT', 'CDN']
OP_TYPE = Literal['EVICT', 'EXISTS', 'GET', 'SET']

logger = logging.getLogger('remote-ops')


class RunStats(NamedTuple):
    """Stats for a given run configuration."""

    backend: BACKEND_TYPE
    op: OP_TYPE
    payload_size_bytes: int | None
    repeat: int
    total_time_ms: float
    avg_time_ms: float
    min_time_ms: float
    max_time_ms: float
    stdev_time_ms: float
    avg_bandwidth_mbps: float | None


async def run_endpoint(
    endpoint: Endpoint,
    remote_endpoint: uuid.UUID | None,
    op: OP_TYPE,
    payload_size: int = 0,
    repeat: int = 3,
) -> RunStats:
    """Run test for single operation and measure performance.

    Args:
        endpoint (Endpoint): local endpoint.
        remote_endpoint (UUID): UUID of remote endpoint to peer with.
        op (str): endpoint operation to test.
        payload_size (int): bytes to send/receive for GET/SET operations.
        repeat (int): number of times to repeat operation. If repeat is greater
            than or equal to three, the slowest and fastest times will be
            dropped to account for the first op being slower while establishing
            a connection.

    Returns:
        RunStats with summary of test run.
    """
    logger.log(TESTING_LOG_LEVEL, f'starting endpoint peering test for {op}')

    if op == 'EVICT':
        times_ms = await endpoint_ops.test_evict(
            endpoint,
            remote_endpoint,
            repeat,
        )
    elif op == 'EXISTS':
        times_ms = await endpoint_ops.test_exists(
            endpoint,
            remote_endpoint,
            repeat,
        )
    elif op == 'GET':
        times_ms = await endpoint_ops.test_get(
            endpoint,
            remote_endpoint,
            payload_size,
            repeat,
        )
    elif op == 'SET':
        times_ms = await endpoint_ops.test_set(
            endpoint,
            remote_endpoint,
            payload_size,
            repeat,
        )
    else:
        raise AssertionError(f'Unsupported operation {op}')

    if len(times_ms) >= 3:
        times_ms = times_ms[1:-1]

    avg_time_s = sum(times_ms) / 1000 / len(times_ms)
    payload_mb = payload_size / 1e6
    avg_bandwidth_mbps = (
        payload_mb / avg_time_s if op in ('GET', 'SET') else None
    )

    return RunStats(
        backend='ENDPOINT',
        op=op,
        payload_size_bytes=payload_size if op in ('GET', 'SET') else None,
        repeat=repeat,
        total_time_ms=sum(times_ms),
        avg_time_ms=sum(times_ms) / len(times_ms),
        min_time_ms=min(times_ms),
        max_time_ms=max(times_ms),
        stdev_time_ms=(
            statistics.stdev(times_ms) if len(times_ms) > 1 else 0.0
        ),
        avg_bandwidth_mbps=avg_bandwidth_mbps,
    )


def run_cdn(
    store : Store,
    op: OP_TYPE,
    payload_size: int = 0,
    repeat: int = 3,
) -> RunStats:
    """Run test for single operation and measure performance.

    Args:
        store (Store): Store connected to remote server.
        op (str): endpoint operation to test.
        payload_size (int): bytes to send/receive for GET/SET operations.
        repeat (int): number of times to repeat operation. If repeat is greater
            than or equal to three, the slowest and fastest times will be
            dropped to account for the first op being slower while establishing
            a connection.

    Returns:
        RunStats with summary of test run.
    """
    logger.log(TESTING_LOG_LEVEL, f'starting remote cdn test for {op}')

    if op == 'EVICT':
        times_ms = cdn_ops.test_evict(store, repeat)
    elif op == 'EXISTS':
        times_ms = cdn_ops.test_exists(store, repeat)
    elif op == 'GET':
        times_ms = cdn_ops.test_get(store, payload_size, repeat)
    elif op == 'SET':
        times_ms = cdn_ops.test_set(store, payload_size, repeat)
    else:
        raise AssertionError(f'Unsupported operation {op}')

    if len(times_ms) >= 3:
        times_ms = times_ms[1:-1]

    avg_time_s = sum(times_ms) / 1000 / len(times_ms)
    payload_mb = payload_size / 1e6
    avg_bandwidth_mbps = (
        payload_mb / avg_time_s if op in ('GET', 'SET') else None
    )

    return RunStats(
        backend='CDN',
        op=op,
        payload_size_bytes=payload_size if op in ('GET', 'SET') else None,
        repeat=repeat,
        total_time_ms=sum(times_ms),
        avg_time_ms=sum(times_ms) / len(times_ms),
        min_time_ms=min(times_ms),
        max_time_ms=max(times_ms),
        stdev_time_ms=(
            statistics.stdev(times_ms) if len(times_ms) > 1 else 0.0
        ),
        avg_bandwidth_mbps=avg_bandwidth_mbps,
    )


async def runner_endpoint(
    remote_endpoint: uuid.UUID | None,
    ops: list[OP_TYPE],
    *,
    payload_sizes: list[int],
    repeat: int,
    server: str | None = None,
    csv_file: str | None = None,
) -> None:
    """Run matrix of test test configurations with an Endpoint.

    Args:
        remote_endpoint (UUID): remote endpoint UUID to peer with.
        ops (str): endpoint operations to test.
        payload_sizes (int): bytes to send/receive for GET/SET operations.
        repeat (int): number of times to repeat operations.
        server (str): relay server address
        csv_file (str): optional csv filepath to log results to.
    """
    if csv_file is not None:
        csv_logger = CSVLogger(csv_file, RunStats)

    async with Endpoint(
        name=socket.gethostname(),
        uuid=uuid.uuid4(),
        relay_server=server,
        max_object_size=10000000000,
    ) as endpoint:
        print(endpoint._max_object_size)
        for op in ops:
            for i, payload_size in enumerate(payload_sizes):
                # Only need to repeat for payload_size for GET/SET
                if i == 0 or op in ['GET', 'SET']:
                    run_stats = await run_endpoint(
                        endpoint,
                        remote_endpoint=remote_endpoint,
                        op=op,
                        payload_size=payload_size,
                        repeat=repeat,
                    )

                    logger.log(TESTING_LOG_LEVEL, run_stats)
                    if csv_file is not None:
                        csv_logger.log(run_stats)

    if csv_file is not None:
        csv_logger.close()
        logger.log(TESTING_LOG_LEVEL, f'results logged to {csv_file}')


def runner_cdn(
    cdn_address: str,
    usertoken: str,
    catalog: str,
    ops: list[OP_TYPE],
    *,
    payload_sizes: list[int],
    repeat: int,
    csv_file: str | None = None,
) -> None:
    """Run matrix of test test configurations with a CDN server.

    Args:
        cdn_address (str): remote CDN server hostname/IP.
        usertoken (str): user credentials.
        catalog (str): catalog to store the data.
        ops (str): endpoint operations to test.
        payload_sizes (int): bytes to send/receive for GET/SET operations.
        repeat (int): number of times to repeat operations.
        csv_file (str): optional csv filepath to log results to.
    """
    if csv_file is not None:
        csv_logger = CSVLogger(csv_file, RunStats)

    conn = CDNConnector(catalog=catalog, user_token=usertoken, gateway=cdn_address)
    store = Store('my-store', conn)
    print(payload_sizes)
    for op in ops:
        for i, payload_size in enumerate(payload_sizes):
            # Only need to repeat for payload_size for GET/SET
            if i == 0 or op in ['GET', 'SET']:
                run_stats = run_cdn(
                    store,
                    op=op,
                    payload_size=payload_size,
                    repeat=repeat,
                )

                logger.log(TESTING_LOG_LEVEL, run_stats)
                if csv_file is not None:
                    csv_logger.log(run_stats)

    if csv_file is not None:
        csv_logger.close()
        logger.log(TESTING_LOG_LEVEL, f'results logged to {csv_file}')


def main(argv: Sequence[str] | None = None) -> int:
    """Remote ops test entrypoint."""
    argv = argv if argv is not None else sys.argv[1:]

    parser = argparse.ArgumentParser(
        description='Remote ops performance test.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        'backend',
        choices=['ENDPOINT', 'CDN'],
        help='Remote objects store backend to test',
    )
    parser.add_argument(
        '--endpoint',
        required='ENDPOINT' in sys.argv,
        help='Remote Endpoint UUID',
    )
    parser.add_argument(
        '--cdn',
        required='CDN' in sys.argv,
        help='CDN server hostname/IP',
    )
    parser.add_argument(
        '--cdn-catalog',
        required='CDN' in sys.argv,
        help='Catalog to store the data',
    )
    parser.add_argument(
        '--cdn-usertoken',
        required='CDN' in sys.argv,
        help='User token to access to CDN services',
    )
    parser.add_argument(
        '--ops',
        choices=['GET', 'SET', 'EXISTS', 'EVICT'],
        nargs='+',
        required=True,
        help='Endpoint operations to measure',
    )
    parser.add_argument(
        '--payload-sizes',
        type=int,
        nargs='+',
        default=[10],
        help='Payload sizes for GET/SET operations',
    )
    parser.add_argument(
        '--server',
        required='ENDPOINT' in sys.argv,
        help='Relay server address for connecting to the remote endpoint',
    )
    parser.add_argument(
        '--repeat',
        type=int,
        default=10,
        help='Number of times to repeat operations',
    )
    parser.add_argument(
        '--no-uvloop',
        action='store_true',
        help='Override using uvloop if available (for ENDPOINT backend only)',
    )
    add_logging_options(parser)
    args = parser.parse_args(argv)

    init_logging(args.log_file, args.log_level, force=True)

 
    if args.backend == 'ENDPOINT':
        if not args.no_uvloop:
            try:
                import uvloop

                uvloop.install()
                logger.info('uvloop available... using as event loop')
            except ImportError:  # pragma: no cover
                logger.info('uvloop unavailable... using asyncio event loop')
        else:
            logger.info('uvloop override... using asyncio event loop')

        asyncio.run(
            runner_endpoint(
                uuid.UUID(args.endpoint),
                args.ops,
                payload_sizes=args.payload_sizes,
                repeat=args.repeat,
                server=args.server,
                csv_file=args.csv_file,
            ),
        )
    elif args.backend == 'CDN':
        runner_cdn(
            args.cdn,
            args.cdn_usertoken,
            args.cdn_catalog,
            args.ops,
            payload_sizes=args.payload_sizes,
            repeat=args.repeat,
            csv_file=args.csv_file,
        )
    else:
        raise AssertionError('Unreachable.')

    return 0

if __name__ == '__main__':
    main()