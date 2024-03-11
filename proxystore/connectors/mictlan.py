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

from proxystore.cdn import p2p

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self
    
class MictlanXError(Exception):
    """Exception resulting from request to CDN."""

    pass

class MictlanKey(NamedTuple):
    """Key to objects store in a CDN server.

    Attributes:
        cdn_key: Unique object ID.
    """

    ball_id: str
    bucket_id: str
    
class MictlanConnector:
    def __init__(self, 
                 bucket_id : str, 
                 peers: str,
                 client_id : str = "client-0",
                 workers : int = 1,
                 lb_algorithm : str = "2CHOICES_UF"
                ):
        self.peers = list(peers)
        self.bucket_id = bucket_id
        self.client_id = client_id
        self.workers = workers
        self.lb_algorithm = lb_algorithm
        
        # Maintain single session for connection pooling persistence to
        # speed up repeat requests to same endpoint.
        self._session = requests.Session()
        
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
            'bucket_id': self.bucket_id,
            'client_id': self.client_id,
            'workers': self.workers,
            'lb_algorithm': self.lb_algorithm
        }
        
    @classmethod
    def from_config(cls, config: dict[str, Any]) -> MictlanConnector:
        """Create a new connector instance from a configuration.

        Args:
            config: Configuration returned by `#!python .config()`.
        """
        return cls(**config)
    
    def evict(
        self,
        key: MictlanKey
    ) -> None:
        try:    
            p2p.evict(
                peers = self.peers,
                key = key.ball_id,
                bucket_id = key.bucket_id,
                client_id = self.client_id,
                workers = self.workers
            )
        except requests.exceptions.RequestException as e:
            #assert e.response is not None
            raise MictlanXError(
                f'Evict failed with error code {e.response.status_code}.',
            )(e)
    
    def exists(
        self,
        key: MictlanKey
    ) -> bool:
        try:    
            return p2p.exists(
                peers = self.peers,
                key = key.ball_id,
                bucket_id = key.bucket_id,
                client_id = self.client_id,
                workers = self.workers
            )
        except requests.exceptions.RequestException as e:
            #assert e.response is not None
            raise MictlanXError(
                f'Exists failed with error code {e.response.status_code}.',
            )(e)
    
    def get(
        self,
        key: MictlanKey
    ) -> bytes | None:
        try:    
            return p2p.get(
                peers = self.peers,
                key = key.ball_id,
                bucket_id = key.bucket_id,
                client_id = self.client_id,
                workers = self.workers
            )
        except requests.exceptions.RequestException as e:
            #assert e.response is not None
            raise MictlanXError(
                f'Get failed with error code {e.response.status_code}.',
            )(e)


    
    def put(
        self,
        data: bytes
    ) -> None:
        
        # calculate the object id
        object_id = MictlanKey(ball_id=str(uuid.uuid4()), bucket_id=self.bucket_id)

        p2p.put(
            data = data,
            peers = self.peers,
            bucket_id = object_id.bucket_id,
            key = object_id.ball_id,
            client_id = self.client_id,
            workers = self.workers,
            lb_algorithm = self.lb_algorithm
        )
        
        return object_id
    
    def set(
        self,
        key: MictlanKey,
        data: bytes
    ) -> None:
        try:    
            p2p.put(
                data = data,
                peers = self.peers,
                key = key.ball_id,
                bucket_id = key.bucket_id,
                client_id = self.client_id,
                workers = self.workers,
                lb_algorithm = self.lb_algorithm
            )
        except requests.exceptions.RequestException as e:
            #assert e.response is not None
            raise MictlanXError(
                f'Set failed with error code {e.response.status_code}.',
            )(e)
    
    

        