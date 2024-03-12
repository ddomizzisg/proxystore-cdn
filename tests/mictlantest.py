from proxystore.connectors.mictlan import MictlanConnector
from mictlanx.utils.index import Utils
import os
from proxystore.connectors.mictlan import MictlanKey
import time

routers = Utils.routers_from_str(
        routers_str = "mictlanx-router-0:alpha.tamps.cinvestav.mx/v0/mictlanx/router:-1",
        protocol    = "https"
    )

conn = MictlanConnector(
    routers=routers,
    bucket_id=os.environ.get("MICTLANX_BUCKET_ID","pruebasproxystore"),
    client_id=os.environ.get("MICTLANX_CLIENT_ID","client-0"),
    workers=int(os.environ.get("MICTLANX_WORKERS",1)),
    lb_algorithm=os.environ.get("MICTLANX_LB_ALGORITHM","2CHOICES_UF")
)

data = b"Hello World"

key = conn.put(data)
data = conn.get(key)
print(conn.exists(key))
