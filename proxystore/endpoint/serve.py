"""CLI for serving an endpoint as a REST server."""
from __future__ import annotations

import json
import logging
import os
import uuid

import quart
from quart import request
from quart import Response

from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.exceptions import PeerRequestError

logger = logging.getLogger(__name__)

# Override Quart standard handlers
quart.logging.default_handler = logging.NullHandler()  # type: ignore
quart.logging.serving_handler = logging.NullHandler()  # type: ignore


def create_app(
    endpoint: Endpoint,
    max_content_length: int = 1000 * 1000 * 1000,
    body_timeout: int = 300,
) -> quart.Quart:
    """Creates quart app for endpoint and registers routes.

    Args:
        endpoint (Endpoint): initialized endpoint to forward quart routes to.
        max_content_length (int): max request body size in bytes
            (default: 1 GB).
        body_timeout (int): number of seconds to wait for the body to be
            completely received (default: 300)

    Returns:
        Quart app.
    """
    app = quart.Quart(__name__)

    # Propagate custom handlers to Quart App and Serving loggers
    app_logger = quart.logging.create_logger(app)
    serving_logger = quart.logging.create_serving_logger()
    app_logger.handlers = logger.handlers
    serving_logger.handlers = logger.handlers

    @app.before_serving
    async def startup() -> None:
        await endpoint.async_init()

    @app.after_serving
    async def shutdown() -> None:
        await endpoint.close()

    @app.route('/')
    async def home() -> tuple[str, int]:
        return ('', 200)

    @app.route('/endpoint', methods=['GET'])
    async def endpoint_() -> Response:
        return Response(
            json.dumps({'uuid': str(endpoint.uuid)}),
            200,
            content_type='application/json',
        )

    @app.route('/evict', methods=['POST'])
    async def evict() -> Response:
        key = request.args.get('key', None)
        if key is None:
            return Response('request missing key', 400)

        endpoint_uuid: str | uuid.UUID | None = request.args.get(
            'endpoint',
            None,
        )
        if isinstance(endpoint_uuid, str):
            try:
                endpoint_uuid = uuid.UUID(endpoint_uuid, version=4)
            except ValueError:
                return Response(f'{endpoint_uuid} is not a valid UUID4', 400)

        try:
            await endpoint.evict(key=key, endpoint=endpoint_uuid)
            return Response('', 200)
        except PeerRequestError as e:
            return Response(str(e), 400)

    @app.route('/exists', methods=['GET'])
    async def exists() -> Response:
        key = request.args.get('key', None)
        if key is None:
            return Response('request missing key', 400)

        endpoint_uuid: str | uuid.UUID | None = request.args.get(
            'endpoint',
            None,
        )
        if isinstance(endpoint_uuid, str):
            try:
                endpoint_uuid = uuid.UUID(endpoint_uuid, version=4)
            except ValueError:
                return Response(f'{endpoint_uuid} is not a valid UUID4', 400)

        try:
            exists = await endpoint.exists(key=key, endpoint=endpoint_uuid)
            return Response(
                json.dumps({'exists': exists}),
                200,
                content_type='application/json',
            )
        except PeerRequestError as e:
            return Response(str(e), 400)

    @app.route('/get', methods=['GET'])
    async def get() -> Response:
        key = request.args.get('key', None)
        if key is None:
            return Response('request missing key', 400)

        endpoint_uuid: str | uuid.UUID | None = request.args.get(
            'endpoint',
            None,
        )
        if isinstance(endpoint_uuid, str):
            try:
                endpoint_uuid = uuid.UUID(endpoint_uuid, version=4)
            except ValueError:
                return Response(f'{endpoint_uuid} is not a valid UUID4', 400)

        try:
            data = await endpoint.get(key=key, endpoint=endpoint_uuid)
        except PeerRequestError as e:
            return Response(str(e), 400)

        if data is not None:
            return Response(
                response=data,
                content_type='application/octet-stream',
            )
        else:
            return Response('', 400)

    @app.route('/set', methods=['POST'])
    async def set() -> Response:
        key = request.args.get('key', None)
        if key is None:
            return Response('request missing key', 400)

        endpoint_uuid: str | uuid.UUID | None = request.args.get(
            'endpoint',
            None,
        )
        if isinstance(endpoint_uuid, str):
            try:
                endpoint_uuid = uuid.UUID(endpoint_uuid, version=4)
            except ValueError:
                return Response(f'{endpoint_uuid} is not a valid UUID4', 400)

        try:
            await endpoint.set(
                key=key,
                data=await request.get_data(),
                endpoint=endpoint_uuid,
            )
            return Response('', 200)
        except PeerRequestError as e:
            return Response(str(e), 400)

    logger.info(
        'quart routes registered to endpoint '
        f'{endpoint.uuid} ({endpoint.name})',
    )

    app.config['MAX_CONTENT_LENGTH'] = max_content_length
    app.config['BODY_TIMEOUT'] = body_timeout

    return app


def serve(
    name: str,
    uuid: uuid.UUID,
    host: str,
    port: int,
    server: str | None = None,
    log_level: int | str = logging.INFO,
    log_file: str | None = None,
) -> None:
    """Initialize endpoint and serve Quart app.

    Warning:
        This function does not return until the Quart app is terminated.

    Args:
        name (str): name of endpoint.
        uuid (str): uuid of endpoint.
        host (str): host address to server Quart app on.
        port (int): port to serve Quart app on.
        server (str): address of signaling server that endpoint
            will register with and use for establishing peer to peer
            connections. If None, endpoint will operate in solo mode (no
            peering) (default: None).
        log_level (int): logging level of endpoint (default: INFO).
        log_file (str): optional file path to append log to.
    """
    if log_file is not None:
        parent_dir = os.path.dirname(log_file)
        if not os.path.isdir(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)
        logging.getLogger().handlers.append(logging.FileHandler(log_file))

    for handler in logging.getLogger().handlers:
        handler.setFormatter(
            logging.Formatter(
                '[%(asctime)s.%(msecs)03d] %(levelname)-5s (%(name)s) :: '
                '%(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
            ),
        )
    logging.getLogger().setLevel(log_level)

    endpoint = Endpoint(name=name, uuid=uuid, signaling_server=server)
    app = create_app(endpoint)

    logger.info(
        f'serving endpoint {endpoint.uuid} ({endpoint.name}) on {host}:{port}',
    )
    app.run(host=host, port=port)
