from mictlanx.v4.client import Client
from mictlanx.utils.index import Utils
from option import Result
import requests

def evict(
    peers: list,
    key: str,
    bucket_id: str = "default2",
    client_id: str = "client-0",
    workers: int = 1,
    lb_algorithm: str = "2CHOICES_UF",
) -> None:
    client = Client(
        client_id    = client_id,
        peers        = peers,
        debug        = False,
        daemon       = True, 
        disable_log = True ,
        lb_algorithm = lb_algorithm,
        max_workers  = workers,
        bucket_id= bucket_id 
    )
    
    #res = client.evict(key = key, bucket_id=bucket_id)
    
    # Get result from future
    #response = res.result()
    
    if response:
        return True
    else:
        raise requests.exceptions.RequestException(
            f'Peer returned an error. '
            f'{response}'
        )

def exists(
    peers: list,
    key: str,
    bucket_id: str = "default2",
    client_id: str = "client-0",
    workers: int = 1,
    lb_algorithm: str = "2CHOICES_UF",
) -> bool:
    
    client = Client(
        client_id    = client_id,
        peers        = peers,
        debug        = False,
        daemon       = True, 
        disable_log = True ,
        lb_algorithm = lb_algorithm,
        max_workers  = workers,
        bucket_id= bucket_id 
    )
    
    res = client.get(key = key, bucket_id=bucket_id)
    
    # Get result from future
    response = res.result()
    
    if response:
        return True
    else:
        False

def get(
    peers: list,
    key: str,
    bucket_id: str = "default2",
    client_id: str = "client-0",
    workers: int = 1,
    lb_algorithm: str = "2CHOICES_UF",
) -> bytes | None:
    print(peers)
    client = Client(
        client_id    = client_id,
        peers        = peers,
        debug        = False,
        daemon       = True, 
        disable_log = True ,
        lb_algorithm = lb_algorithm,
        max_workers  = workers,
        bucket_id= bucket_id 
    )
    
    res = client.get(key = key, bucket_id=bucket_id)
    
    # Get result from future
    response = res.result()
    
    if response:
        return response.unwrap().value
    else:
        raise Exception(response)

def put(
    data: bytes,
    peers: list,
    bucket_id: str = "default2",
    client_id: str = "client-0",
    key: str = "ball-0",
    workers: int = 1,
    lb_algorithm: str = "2CHOICES_UF",
) -> None:
    
    client = Client(
        client_id    = client_id,
        peers        = peers,
        debug        = False,
        daemon       = True, 
        disable_log = True ,
        max_workers  = workers,
        lb_algorithm =lb_algorithm,
        bucket_id= bucket_id 
    )

    # Cual es la diferencia entre el ball_id y el key?
    res = client.put(data, bucket_id=bucket_id, checksum_as_key=False, key=key)
    
    # Get result from future
    response = res.result()
    
    if response:
        return True
    else:
        raise Exception(response)
    