import requests
import uuid
from proxystore.utils.data import chunk_bytes
from proxystore.cdn.constants import MAX_CHUNK_LENGTH
from proxystore.cdn.reliability.ida import split_bytes
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
import pickle
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
    max_workers: int = 1,
) -> None:
    
    if chunks > 1:
        disperse = "IDA"
        return put_chunks(**locals())
    else:
        post = requests.post if session is None else session.post
        disperse = "SINGLE"
        
        start = time.perf_counter_ns()
        response = post(
            f'http://{address}/api/files/push',
            params={"name": name, "size": len(data), "hash": data_hash, "key": key,
                    "tokenuser": token_user, "catalog": catalog,
                    "is_encrypted": int(is_encrypted), "chunks": chunks,
                    "required_chunks": required_chunks, "disperse": disperse}
        )
        
        if response.status_code == 201:
            storage_node = response.json()["nodes"][0]["route"]
            end = time.perf_counter_ns()
            metadata_time = (end - start)  / 1e6
            
            start = time.perf_counter_ns()
            upload_to_storage_node(storage_node, data, token_user, session)
            end = time.perf_counter_ns()
            data_upload_time = (end - start)  / 1e6
            
        else:
            raise requests.exceptions.RequestException(
                f'Metadata server returned HTTP error code {response.status_code}. '
                f'{response.text}',
                response=response,
            )
    
    return {"metadata_time": metadata_time, "data_upload_time": data_upload_time}

def put_chunks(
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
    max_workers: int = 1, 
    disperse: str = "IDA"
) -> None:
    
    start = time.perf_counter_ns()
    response = regist_on_metadata(
        address, name, data, token_user, data_hash, key, catalog, 
        session, is_encrypted, chunks, required_chunks, disperse
    )
    
    if response.status_code == 201:
        servers_urls = response.json()["nodes"]
        end = time.perf_counter_ns()
        metadata_time = (end - start)  / 1e6
        
        
        start = time.perf_counter_ns()
        data_chunks = split_bytes(data, chunks, required_chunks)
        end = time.perf_counter_ns()
        dispersal_time = (end - start)  / 1e6
            
        start = time.perf_counter_ns()
        if max_workers > 1:
            #with ThreadPoolExecutor(max_workers=max_workers) as executor:
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                results = [
                    executor.submit(
                        upload_to_storage_node,
                        servers_urls[i]["route"],
                        pickle.dumps(data_chunks[i]),
                        token_user,
                        session
                    )
                    for i in range(chunks)
                ]
                
                for result in results:
                    if not result.result():
                        raise requests.exceptions.RequestException(
                            f'Failed to upload chunk to storage node {result.result()}.'
                        )
        else:
            results = [
                upload_to_storage_node(
                    servers_urls[i]["route"], 
                    pickle.dumps(data_chunks[i]), 
                    token_user, session) 
                for i in range(chunks)]
    else:
        raise requests.exceptions.RequestException(
            f'Metadata server returned HTTP error code {response.status_code}. '
            f'{response.text}',
            response=response,
        )
        
    end = time.perf_counter_ns()
    
    data_upload_time = (end - start)  / 1e6 
    
    return {"metadata_time": metadata_time, "dispersal_time": dispersal_time, "data_upload_time": data_upload_time}

def regist_on_metadata(
    address: str,
    name: str,
    data: bytes,
    token_user: str,
    data_hash: str,
    key: str,
    catalog: str,
    session: requests.Session | None = None,
    is_encrypted: bool = False,
    chunks: int = 1,
    required_chunks: int = 1,
    disperse: str = "SINGLE"
) -> requests.Response:
    
    post = requests.post if session is None else session.post
    response = post(
        f'http://{address}/api/files/push',
        params={"name": name, "size": len(data), "hash": data_hash, "key": key,
                "tokenuser": token_user, "catalog": catalog,
                "is_encrypted": int(is_encrypted), "chunks": chunks,
                "required_chunks": required_chunks, "disperse": disperse}
    )
    return response
    

def upload_to_storage_node(
    url: str,
    data: bytes,
    token_user: str,
    session: requests.Session | None = None,
) -> bool:
    
    post = requests.post if session is None else session.post
    response = post(
        f'http://{url}',
        headers={'Content-Type': 'application/octet-stream'},
        params={'tokenuser': token_user},
        data=data,
        stream=True,
    )
    
    if not response.ok:
        raise requests.exceptions.RequestException(
            f'Storage node {url} returned HTTP error code {response.status_code}. '
            f'{response.text}',
            response=response,
        )
        
    else:
        return True