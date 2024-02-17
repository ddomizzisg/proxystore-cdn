from mictlanx.v4.client import Client
from mictlanx.utils.index import Utils
from option import Result

def put(
    data: bytes,
    peers: str,
    bucket_id: str = "default",
    client_id: str = "client-0",
    workers: int = 1,
    lb_algorithm: str = "2CHOICES_UF",
) -> None:
    peers =  Utils.peers_from_str_v2(
            peers_str=peers, 
            protocol="https"
        )
    
    client = Client(
        client_id    = "client-0",
        peers        = list(peers),
        debug        = False,
        daemon       = True, 
        disable_log = True ,
        max_workers  = 2,
        lb_algorithm ="2CHOICES_UF",
        bucket_id= bucket_id 
    )
    
    res = client.put(data, bucket_id=bucket_id)
    
    if isinstance(res, Result):
        pass
