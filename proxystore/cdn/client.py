import requests
import uuid
from proxystore.utils.data import chunk_bytes
from proxystore.cdn.constants import MAX_CHUNK_LENGTH

import time

def evict(
    address: str,
    key: str,
    token_user: str = None,
    session: requests.Session | None = None
) -> None:
    delete_ = requests.delete if session is None else session.delete
    response = delete_(
        f'http://{address}/api/files/{token_user}/delete/{key}'
    )
    
    if not response.ok:
        raise requests.exceptions.RequestException(
            f'Server returned HTTP error code {response.status_code}. '
            f'{response.text}',
            response=response,
        )
        
def exists(
    address: str,
    key: str,
    token_user: str = None,
    session: requests.Session | None = None
) -> bool:
    get_ = requests.get if session is None else session.get
    response = get_(
        f'http://{address}/api/files/{token_user}/exists/{key}'
    )

    if not response.ok:
        raise requests.exceptions.RequestException(
            f'Server returned HTTP error code {response.status_code}. '
            f'{response.text}',
            response=response,
        )

    return response.json()["exists"]


def get(
        address: str,
        key: str,
        token_user: str = None,
        session: requests.Session | None = None
) -> bytes | None:
    post = requests.post if session is None else session.post
    response = post(
        f'http://{address}/api/files/pull',
        params={"key": key,
                "tokenuser": token_user}
    )

    if response.status_code == 200:
        route = response.json()["data"]["routes"][0]["route"]
        get_ = requests.get if session is None else session.get
        response = get_(
            f'http://{route}',
            stream=True,
        )

        # Status code 404 is only returned if there's no data associated with the
        # provided key.
        if response.status_code == 404:
            return None

        if not response.ok:
            raise requests.exceptions.RequestException(
                f'Endpoint returned HTTP error code {response.status_code}. '
                f'{response.text}',
                response=response,
            )

        data = bytearray()
        for chunk in response.iter_content(chunk_size=None):
            data += chunk
        return bytes(data)


def put(
    address: str,
    key: str,
    data_hash: str,
    name: str,
    data: bytes,
    token_user: str,
    catalog: str,
    session: requests.Session | None = None,
    is_encrypted: bool = False,
    chunks: int = 1,
    required_chunks: int = 1,
    disperse: str = "SINGLE"

) -> None:
    post = requests.post if session is None else session.post
    
    start = time.perf_counter_ns()
    
    response = post(
        f'http://{address}/api/files/push',
        params={"name": name, "size": len(data), "hash": data_hash, "key": key,
                "tokenuser": token_user, "catalog": catalog,
                "is_encrypted": int(is_encrypted), "chunks": chunks,
                "required_chunks": required_chunks, "disperse": disperse}
    )

    end = time.perf_counter_ns()
    print(f"Time to get route: {(end - start) / 1e6} ms")
    
    if response.status_code == 201:
        storage_node = response.json()["nodes"][0]["route"]
        response = post(
            f'http://{storage_node}',
            headers={'Content-Type': 'application/octet-stream'},
            params={'tokenuser': token_user},
            data=chunk_bytes(data, MAX_CHUNK_LENGTH),
            stream=True,
        )
        #print(response.text)
        
        if not response.ok:
            raise requests.exceptions.RequestException(
                f'Storage node {storage_node} returned HTTP error code {response.status_code}. '
                f'{response.text}',
                response=response,
            )
    else:
        raise requests.exceptions.RequestException(
            f'Metadata server returned HTTP error code {response.status_code}. '
            f'{response.text}',
            response=response,
        )

