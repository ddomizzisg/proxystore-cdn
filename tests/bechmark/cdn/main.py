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
import concurrent.futures
import os
import random

from proxystore.endpoint.endpoint import Endpoint

import endpoint_ops as endpoint_ops
import cdn_ida as ops_ida
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
        total_time_ms=0,
        avg_time_ms=0,
        min_time_ms=0,
        max_time_ms=0,
        stdev_time_ms=(
            statistics.stdev(times_ms) if len(times_ms) > 1 else 0.0
        ),
        avg_bandwidth_mbps=avg_bandwidth_mbps,
    )


def run_cdn(
    conn: CDNConnector,
    op: OP_TYPE,
    payload_size: int = 0,
    repeat: int = 3,
    clients: int = 1,
    nodes: int = 1,
    k: int = 1,
    workers: int = 1,
    files = None
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
        times_ms = ops_ida.test_evict(conn, repeat)
    elif op == 'EXISTS':
        times_ms = ops_ida.test_exists(conn, repeat)
    elif op == 'GET':
        times_ms = ops_ida.test_get(
            conn, payload_size, repeat, nodes, k, workers)
    elif op == 'SET':
        if files is not None:
            #print(files)
            times_ms = ops_ida.test_set_files(
                conn, files, repeat, nodes, k, workers)
            payload_size = sum([os.path.getsize(f) for f in files])
        else:
            times_ms = ops_ida.test_set(
                conn, payload_size, repeat, nodes, k, workers)
        
        if len(times_ms) > 0:
            total_time = sum([x["total_time"] for x in times_ms])
            #print("entro", total_time, len(times_ms))
            avg_total_time = total_time / len(times_ms)
            #avg_metadata_time = sum([x["metadata_time"]
            #                        for x in times_ms]) / len(times_ms)
            #avg_data_upload_time = sum([x["data_upload_time"]
            #                           for x in times_ms]) / len(times_ms)
            payload_mb = payload_size / 1e6
            throughput = payload_mb / avg_total_time
            throughput_upload = payload_mb / (total_time / 1000)

            return RunStats(
                backend='CDN',
                op=op,
                payload_size_bytes=payload_size if op in ('GET', 'SET') else None,
                clients=clients,
                repeat=repeat,
                n=nodes,
                k=k,
                workers=workers,
                total_time_ms=total_time,
                avg_time_ms=avg_total_time,
                min_time_ms=min([x["total_time"] for x in times_ms]),
                max_time_ms=max([x["total_time"] for x in times_ms]),
                stdev_time_ms=(
                    statistics.stdev([x["total_time"] for x in times_ms]) if len(
                        times_ms) > 1 else 0.0
                ),
                throughput=throughput,
                metadata_time=total_time,
                data_upload_time=total_time,
                throughput_upload=throughput_upload
            )

    else:
        raise AssertionError(f'Unsupported operation {op}')

    if len(times_ms) > 0:
        avg_time_s = sum(times_ms) / 1000 / len(times_ms)
        payload_mb = payload_size / 1e6
        avg_bandwidth_mbps = (
            payload_mb / avg_time_s if op in ('GET', 'SET') else None
        )

        return RunStats(
            backend='CDN',
            op=op,
            payload_size_bytes=payload_size if op in ('GET', 'SET') else None,
            clients=clients,
            repeat=repeat,
            n=nodes,
            k=k,
            workers=workers,
            total_time_ms=sum(times_ms),
            avg_time_ms=sum(times_ms) / len(times_ms),
            min_time_ms=min(times_ms),
            max_time_ms=max(times_ms),
            stdev_time_ms=(
                statistics.stdev(times_ms) if len(times_ms) > 1 else 0.0
            ),
            avg_bandwidth_mbps=avg_bandwidth_mbps,
        )
    else: 
        return RunStats(
            backend='CDN',
            op=op,
            payload_size_bytes=payload_size if op in ('GET', 'SET') else None,
            clients=clients,
            repeat=repeat,
            n=nodes,
            k=k,
            workers=workers,
            total_time_ms=0,
            avg_time_ms=0,
            min_time_ms=100000000000000,
            max_time_ms=0,
            stdev_time_ms=(
                statistics.stdev(times_ms) if len(times_ms) > 1 else 0.0
            ),
            avg_bandwidth_mbps=0,
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


def two_choices(files, workers):
    utilization = [0 for _ in range(workers)]
    distribution = [[] for _ in range(workers)]
    for f in files:
        #print(f)
        # random 1
        r1 = random.randint(0, workers-1)

        # random 2
        r2 = random.randint(0, workers-1)

        while r1 == r2:
            r2 = random.randint(0, workers-1)

        if utilization[r1] < utilization[r2]:
            # get file size
            file_size = os.path.getsize(f)
            utilization[r1] += file_size
            distribution[r1].append(f)
        else:
            file_size = os.path.getsize(f)
            utilization[r2] += file_size
            distribution[r2].append(f)
    return distribution


def runner_cdn_concurrent(
    cdn_address: str,
    usertoken: str,
    catalog: str,
    ops: list[OP_TYPE],
    *,
    payload_sizes: list[int],
    clients: int,
    chunks: int,
    repeat: int,
    csv_file: str | None = None,
    files: str | None = None, 
    parallel: bool = True
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

    for op in ops:
        if files is not None:
            # read files in input dir
            result = [os.path.join(dp, f) for dp, dn, filenames in os.walk(
                files) for f in filenames]
            # result = two_choices(result, chunks) if clients > 1 else [result]
            # for r in result:
            for c in clients:
                #print(result)
                if parallel:
                    lists = two_choices(result, c) if c > 1 else [result]
                else:
                    lists = [result for _ in range(c)]
                conn = CDNConnector(
                    catalog=catalog, user_token=usertoken, gateway=cdn_address)
                with concurrent.futures.ThreadPoolExecutor(max_workers=c) as executor:
                    # print(n)
                    futures = [executor.submit(
                        run_cdn,
                        conn,
                        op=op,
                        files=lists[x],
                        repeat=repeat,
                        clients=c
                    ) for x in range(c)]
                    # Wait for all futures to complete
                    concurrent.futures.wait(futures)

                    run_stats = futures[0] if c == 1 else max(
                        futures, key=lambda x: x.result().max_time_ms)
                    # max_object = max(futures, key=lambda x: x.result().max_time_ms)

                    logger.log(TESTING_LOG_LEVEL, run_stats.result())
                    if csv_file is not None:
                        csv_logger.log(run_stats.result())

        else:
            for i, payload_size in enumerate(payload_sizes):
                for c in clients:
                    for n in range(1, chunks+1):
                        # print(n,chunks)
                        if n > 1:
                            for k in range(1, n):
                                for w in range(1, n+1):
                                    if i == 0 or op in ['GET', 'SET']:
                                        conn = CDNConnector(
                                            catalog=catalog, user_token=usertoken, gateway=cdn_address)
                                        # store = Store('my-store', conn)
                                        with concurrent.futures.ThreadPoolExecutor(max_workers=c) as executor:
                                            # print(n)
                                            futures = [executor.submit(
                                                run_cdn,
                                                conn,
                                                op=op,
                                                payload_size=payload_size,
                                                repeat=repeat,
                                                clients=c,
                                                nodes=n,
                                                k=k,
                                                workers=w
                                            ) for _ in range(c)]
                                            # Wait for all futures to complete
                                            concurrent.futures.wait(futures)

                                            run_stats = futures[0] if c == 1 else max(
                                                futures, key=lambda x: x.result().max_time_ms)
                                            # max_object = max(futures, key=lambda x: x.result().max_time_ms)

                                            logger.log(
                                                TESTING_LOG_LEVEL, run_stats.result())
                                            if csv_file is not None:
                                                csv_logger.log(
                                                    run_stats.result())
                        else:
                            if i == 0 or op in ['GET', 'SET']:
                                conn = CDNConnector(
                                    catalog=catalog, user_token=usertoken, gateway=cdn_address)
                                # store = Store('my-store', conn)
                                with concurrent.futures.ThreadPoolExecutor(max_workers=c) as executor:
                                    futures = [executor.submit(
                                        run_cdn,
                                        conn,
                                        op=op,
                                        payload_size=payload_size,
                                        repeat=repeat,
                                        clients=c
                                    ) for _ in range(c)]
                                    # Wait for all futures to complete
                                    concurrent.futures.wait(futures)

                                    run_stats = futures[0] if c == 1 else max(
                                        futures, key=lambda x: x.result().max_time_ms)
                                    # max_object = max(futures, key=lambda x: x.result().max_time_ms)

                                    logger.log(TESTING_LOG_LEVEL,
                                               run_stats.result())
                                    if csv_file is not None:
                                        csv_logger.log(run_stats.result())

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

    conn = CDNConnector(
        catalog=catalog, user_token=usertoken, gateway=cdn_address)
    store = Store('my-store', conn)
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
        '--chunks',
        type=int,
        default=1,
        help='Number of nodes to use in the CDN'
    )
    parser.add_argument(
        '--parallel-chunks',
        type=bool,
        default=True,
        help='Parallel chunks'
    )
    parser.add_argument(
        '--ops',
        choices=['GET', 'SET', 'EXISTS', 'EVICT'],
        nargs='+',
        required=True,
        help='Endpoint operations to measure',
    )
    parser.add_argument(
        '--files',
        type=str,
        default=None,
        help='Path to the files',
    )
    parser.add_argument(
        '--payload-sizes',
        type=int,
        nargs='+',
        default=[10],
        help='Payload sizes for GET/SET operations',
    )
    parser.add_argument(
        '--clients',
        type=int,
        nargs='+',
        default=[1],
        help='Number of concurrent clients to run the tests',
    )
    
    parser.add_argument(
        '--parallel',
        type=bool,
        default=True,
        help='Parallel execution'
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
        print(args.chunks)
        runner_cdn_concurrent(
            args.cdn,
            args.cdn_usertoken,
            args.cdn_catalog,
            args.ops,
            payload_sizes=args.payload_sizes,
            clients=args.clients,
            chunks=args.chunks,
            repeat=args.repeat,
            csv_file=args.csv_file,
            files=args.files,
            parallel=args.parallel
        )
        # if args.clients > 1:
        #     runner_cdn_concurrent(
        #         args.cdn,
        #         args.cdn_usertoken,
        #         args.cdn_catalog,
        #         args.ops,
        #         payload_sizes=args.payload_sizes,
        #         clients=args.clients,
        #         repeat=args.repeat,
        #         csv_file=args.csv_file,
        #     )
        # else:
        #     runner_cdn(
        #         args.cdn,
        #         args.cdn_usertoken,
        #         args.cdn_catalog,
        #         args.ops,
        #         payload_sizes=args.payload_sizes,
        #         repeat=args.repeat,
        #         csv_file=args.csv_file,
        #     )
    else:
        raise AssertionError('Unreachable.')

    return 0


if __name__ == '__main__':
    main()
