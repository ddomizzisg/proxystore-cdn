from mictlanx.v4.client import Client
from mictlanx.utils.index import Utils
from option import Result
import requests

def evict(
    routers: list,
    key: str,
    bucket_id: str = "default2",
    client_id: str = "client-0",
    workers: int = 1,
    lb_algorithm: str = "2CHOICES_UF",
) -> None:
    client = Client(
        client_id    = client_id,
        routers        = routers,
        debug        = False,
        lb_algorithm = lb_algorithm,
        max_workers  = workers,
        bucket_id= bucket_id 
    )
    
    res = client.delete(key = key, bucket_id=bucket_id)
    
    if res:
        return True
    else:
        raise requests.exceptions.RequestException(
            f'Peer returned an error. '
            f'{res}'
        )

def exists(
    routers: list,
    key: str,
    bucket_id: str = "default2",
    client_id: str = "client-0",
    workers: int = 1,
    lb_algorithm: str = "2CHOICES_UF",
) -> bool:
    
    client = Client(
        client_id    = client_id,
        routers        = routers,
        debug        = False,
        lb_algorithm = lb_algorithm,
        max_workers  = workers,
        bucket_id= bucket_id 
    )
    
    res = client.get_metadata(key = key, bucket_id=bucket_id)
    
    # Get result from future
    response = res.result()
    
    if response:
        return True
    else:
        return False

def get(
    routers: list,
    key: str,
    bucket_id: str = "default2",
    client_id: str = "client-0",
    workers: int = 1,
    lb_algorithm: str = "2CHOICES_UF",
) -> bytes | None:
    
    client = Client(
        client_id    = client_id,
        routers        = routers,
        debug        = False,
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
    routers: list,
    bucket_id: str = "default2",
    client_id: str = "client-0",
    key: str = "ball-0",
    workers: int = 1,
    lb_algorithm: str = "2CHOICES_UF",
) -> None:
    
    client = Client(
        client_id    = client_id,
        routers        = routers,
        debug        = False,
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
    