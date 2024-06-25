from __future__ import annotations

import requests
import configparser
import hashlib
import os
import time
import uuid
import sys

from types import TracebackType
from typing import Any
from typing import NamedTuple
from typing import Sequence

from proxystore.cdn.client import Client

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self


class CDNConnectorError(Exception):
    """Exception resulting from request to CDN."""

    pass


class CDNKey(NamedTuple):
    """Key to objects store in a CDN server.

    Attributes:
        cdn_key: Unique object ID.
    """

    cdn_key: str

class CDNConnector:
    def __init__(self, catalog, user_token=None, gateway=None, configuration_file="config.cfg"):
        self.configuration_file = configuration_file

        # Maintain single session for connection pooling persistence to
        # speed up repeat requests to same endpoint.
        self._session = requests.Session()

        # Load configuration (tokens and url to gateway)
        parser = configparser.RawConfigParser()
        parser.read(configuration_file)
        self.token_user = parser.get('credentials', 'token_user') if user_token is None else user_token
        self.gateway = parser.get('services', 'gateway') if gateway is None else gateway
        self.catalog = catalog
        self.client = Client(self.gateway)
        
    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close tpyhe connector and clean up."""
        self._session.close()

    def config(self) -> dict[str, Any]:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.
        """
        return {
            'catalog': self.catalog,
            'user_token': self.token_user,
            'gateway': self.gateway
        }
        
    @classmethod
    def from_config(cls, config: dict[str, Any]) -> CDNConnector:
        """Create a new connector instance from a configuration.

        Args:
            config: Configuration returned by `#!python .config()`.
        """
        return cls(**config)

    def exists(self, key: CDNKey) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        try:
            return self.client.exists(
                key.cdn_key,
                self.token_user,
                session=self._session,
            )
        except requests.exceptions.RequestException as e:
            assert e.response is not None
            raise CDNConnectorError(
                f'Exists failed with error code {v}.',
            ) from e
            
    def evict(self, key: CDNKey) -> None:
        """Evict an object associated with the key.

        Args:
            key: Key associated with object to evict.
        """
        try:
            self.client.evict(
                key.cdn_key,
                self.token_user,
                session=self._session,
            )
        except requests.exceptions.RequestException as e:
            assert e.response is not None
            raise CDNConnectorError(
                f'Evict failed with error code {v}.',
            ) from e

    def get(self, key: CDNKey):
        try:
            return self.client.get(
                key.cdn_key,
                self.token_user,
                session=self._session,
            )
        except requests.exceptions.RequestException as e:
            assert e.response is not None
            raise CDNConnectorError(
                f'Get failed with error code {e.response.status_code}.',
            )(e)

    def get_batch(self, keys: Sequence[CDNKey]) -> list[bytes | None]:
        """Get a batch of serialized objects associated with the keys.

        Args:
            keys: Sequence of keys associated with objects to retrieve.

        Returns:
            List with same order as `keys` with the serialized objects or \
            `None` if the corresponding key does not have an associated object.
        """
        return [self.get(key) for key in keys]

    def new_key(self) -> CDNKey:
        """Create a new key.

        Warning:
            The returned key will be associated with this instance's local
            endpoint. I.e., when
            [`set()`][proxystore.connectors.endpoint.EndpointConnector.set]
            is called on this key, the connector must be connected to the same
            local endpoint.

        Args:
            obj: Optional object which the key will be associated with.
                Ignored in this implementation.

        Returns:
            Key which can be used to retrieve an object once \
            [`set()`][proxystore.connectors.endpoint.EndpointConnector.set] \
            has been called on the key.
        """
        return CDNKey(cdn_key=str(uuid.uuid4()))

    def put(self, data: bytes = None, filepath: str = None, is_encrypted: bool = False,
            workers: int = 1, resiliency: int = 0) -> CDNKey:
        # Read data from file if data is null and a filepath is received

        name = time.time() if filepath is None else os.path.basename(filepath)

        if data is None and filepath is not None:
            data = open(filepath, mode='rb').read()

        # calculate the object id
        object_id = CDNKey(cdn_key=str(uuid.uuid4()))

        try:
            time_metrics = self.client.put(
                key=object_id.cdn_key,
                name=name,
                data=data,
                token_user=self.token_user,
                catalog=self.catalog,
                session=self._session,
                is_encrypted=is_encrypted,
                max_workers=workers, 
                resiliency=resiliency
            )
        except requests.exceptions.RequestException as e:
            #assert e.response is not None
            raise CDNConnectorError(
                f'Put failed with error code {str(e)}.',
            ) from e

        return object_id#, time_metrics

    def put_batch(self, objs: Sequence[bytes] = None, files: Sequence[CDNKey] = None) -> list[str]:
        """Put a batch of serialized objects in the store.

        Args:
            objs: Sequence of serialized objects to put in the store.
            files: Sequence of paths to reads the files to put in the store

        Returns:
            List of keys with the same order as `objs` which can be used to \
            retrieve the objects.
        """

        if objs is not None:
            return [self.put(data=obj) for obj in objs]
        elif files is not None:
            return [self.put(filepath=file) for file in files]

    def set(self, key: str, obj: bytes, is_encrypted: bool = False,
            number_of_chunks: int = 1, required_chunks: int = 1, disperse="SINGLE") -> None:
        """Set the object associated with a key.

        Note:
            The [`Connector`][proxystore.connectors.protocols.Connector]
            provides write-once, read-many semantics. Thus,
            [`set()`][proxystore.connectors.endpoint.EndpointConnector.set]
            should only be called once per key, otherwise unexpected behavior
            can occur.

        Args:
            key: Key that the object will be associated with.
            obj: Object to associate with the key.
        """
        name = str(time.time())

        # Get the hash of the data
        # encode the string
        sha3_256 = hashlib.sha3_256()
        sha3_256.update(obj)
        obj_sha3_256 = sha3_256.hexdigest()

        try:
            self.client.put(
                address=self.gateway,
                key=key,
                data_hash=obj_sha3_256,
                name=name,
                data=obj,
                token_user=self.token_user,
                catalog=self.catalog,
                session=self._session,
                is_encrypted=is_encrypted,
                chunks=number_of_chunks,
                required_chunks=required_chunks,
                disperse=disperse
            )
        except requests.exceptions.RequestException as e:
            assert e.response is not None
            raise CDNConnectorError(
                f'Put failed with error code {v}.',
            ) from e
            