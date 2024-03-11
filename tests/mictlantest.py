from proxystore.connectors.mictlan import MictlanConnector
from mictlanx.utils.index import Utils
import os
from proxystore.connectors.mictlan import MictlanKey
import time

MICTLANX_PEERS="mictlanx-peer-0:alpha.tamps.cinvestav.mx/v0/mictlanx/peer0:-1 mictlanx-peer-1:alpha.tamps.cinvestav.mx/v0/mictlanx/peer1:-1"
MICTLANX_PROTOCOL="https"

peers =  Utils.peers_from_str_v2(peers_str=os.environ.get("MICTLANX_PEERS",MICTLANX_PEERS) , protocol=os.environ.get("MICTLANX_PROTOCOL",MICTLANX_PROTOCOL)) 

conn = MictlanConnector(
    peers=peers,
    bucket_id=os.environ.get("MICTLANX_BUCKET_ID","pruebasproxystore"),
    client_id=os.environ.get("MICTLANX_CLIENT_ID","client-0"),
    workers=int(os.environ.get("MICTLANX_WORKERS",1)),
    lb_algorithm=os.environ.get("MICTLANX_LB_ALGORITHM","2CHOICES_UF")
)

data = b"Hello World"

key = conn.put(data)
print(key)
time.sleep(2)
data = conn.get(key)
print(data)
print(conn.exists(key))
#key = MictlanKey(ball_id='bbeddf8f-0a85-4cb7-8df0-ad4f756c3daaax', bucket_id='pruebasproxystore')
#conn.set(key,data)
