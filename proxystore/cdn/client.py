import requests
import uuid
from proxystore.utils.data import chunk_bytes
from proxystore.cdn.constants import MAX_CHUNK_LENGTH
from proxystore.cdn.reliability.ida import split_bytes
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
import pickle
import time
import hashlib
import io
import json

class Client(object):
    
    def __init__(self, metadata_server):
        self.metadata_server = metadata_server

    def evict(
        self,
        key: str,
        token_user: str = None,
        session: requests.Session = None
    ) -> None:
        delete_ = requests.delete if session is None else session.delete
        response = delete_(
            f'http://{self.metadata_server}/storage/{token_user}/{key}'
        )
        
        if not response.ok:
            raise requests.exceptions.RequestException(
                f'Server returned HTTP error code {response.status_code}. '
                f'{response.text}',
                response=response,
            )
            
    def exists(
        self,
        key: str,
        token_user: str = None,
        session: requests.Session = None
    ) -> bool:
        
        get_ = requests.get if session is None else session.get
        response = get_(
            f'http://{self.metadata_server}/storage/{token_user}/{key}/exists'
        )
        if not response.ok:
            raise requests.exceptions.RequestException(
                f'Server returned HTTP error code {response.status_code}. '
                f'{response.text}',
                response=response,
            )

        return response.json()["exists"]


    def get(
        self,
        key: str,
        token_user: str = None,
        session: requests.Session = None
    ) -> bytes:
        get = requests.get if session is None else session.get
        #print(key, type(key), sep=" - ")
        response = get(
            f'http://{self.metadata_server}/storage/{token_user}/{key}'
        )
        
        print(response.text)
        
        if response.status_code == 404:
            raise requests.exceptions.RequestException(
                    f'DynoStore returned HTTP error code {response.status_code}. '
                    f'{response.text}',
                    response=response,
                )
        #print(response.status_code, flush=True)
        #print(response.text)
        
        if response.status_code == 200:
            #print(response.json(), flush=True)
            #databytes = bytes(response.json()["data"][0], 'utf-8')
            
            data = bytearray()
            for chunk in response.iter_content(chunk_size=None):
                data += chunk
            #print(data)
            return bytes(data)
    
            #data = response.json["data"]
            #return data    
        
        #     route = response.json()["data"]["routes"][0]["route"]
        #     get_ = requests.get if session is None else session.get
        #     response = get_(
        #         f'http://{route}',
        #         stream=True,
        #     )
            
            

            # Status code 404 is only returned if there's no data associated with the
            # provided key.
            # if response.status_code == 404:
            #     return None

            # if not response.ok:
            #     raise requests.exceptions.RequestException(
            #         f'Endpoint returned HTTP error code {response.status_code}. '
            #         f'{response.text}',
            #         response=response,
            #     )

            # data = bytearray()
            # for chunk in response.iter_content(chunk_size=None):
            #     data += chunk
            # return bytes(data)

    def put(
        self,
        data: bytes,
        token_user: str,
        catalog: str,
        key: str = str(uuid.uuid4()),
        name: str = None,
        session: requests.Session = None,
        is_encrypted: bool = False,
        max_workers: int = 1,
        resiliency: int = 0
    ) -> None:
        start_time = time.perf_counter_ns()
        data_hash = hashlib.sha3_256(data).hexdigest()
        name = data_hash if name is None else name

        put = requests.put if session is None else session.put
        fake_file = io.BytesIO(data)
        
        payload = {"name": name, "size": len(data), "hash": data_hash, "key": key,
                        "is_encrypted": int(is_encrypted), "resiliency": resiliency}
        files   = [
                    ('json', ('payload.json', json.dumps(payload), 'application/json')),
                    ('data', ('data.bin', fake_file, 'application/octet-stream'))
                ]
        response       = requests.put(f'http://{self.metadata_server}/storage/{token_user}/{catalog}/{key}', files=files)

        if response.status_code == 201:
            res = response.json()
        else:
            raise requests.exceptions.RequestException(
                f'Metadata server returned HTTP error code {response.status_code}. '
                f'{response.text}',
                response=response,
            )
        end = time.perf_counter_ns()
        return {"total_time": (end - start_time) / 1e6, "metadata_time": res["total_time"] / 1e6, "upload_time": res["time_upload"] / 1e6}

    def put_chunks(
        self,
        key: str,
        data_hash: str,
        name: str,
        data: bytes,
        token_user: str,
        catalog: str,
        session: requests.Session = None,
        is_encrypted: bool = False,
        chunks: int = 1,
        required_chunks: int = 1,
        max_workers: int = 1, 
        disperse: str = "IDA"
    ) -> None:
        
        start = time.perf_counter_ns()
        response = regist_on_metadata(
            name, data, token_user, data_hash, key, catalog, 
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
        self,
        name: str,
        data: bytes,
        token_user: str,
        data_hash: str,
        key: str,
        catalog: str,
        session: requests.Session = None,
        is_encrypted: bool = False,
        chunks: int = 1,
        required_chunks: int = 1,
        disperse: str = "SINGLE"
    ) -> requests.Response:
        
        post = requests.post if session is None else session.post
        response = post(
            f'http://{self.metadata_server}/api/files/push',
            params={"name": name, "size": len(data), "hash": data_hash, "key": key,
                    "tokenuser": token_user, "catalog": catalog,
                    "is_encrypted": int(is_encrypted), "chunks": chunks,
                    "required_chunks": required_chunks, "disperse": disperse}
        )
        return response
        

    def upload_to_storage_node(
        self,
        url: str,
        data: bytes,
        token_user: str,
        session: requests.Session = None,
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