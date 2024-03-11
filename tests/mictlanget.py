from proxystore.connectors.mictlan import MictlanConnector
from mictlanx.utils.index import Utils
import os

from proxystore.connectors.mictlan import MictlanKey

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

key = MictlanKey(ball_id='bcd2c341-5dc3-4be8-9515-1aaafd98de4b', bucket_id='pruebasproxystore')

data = conn.get(key)
print(data)

