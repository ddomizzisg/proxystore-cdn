from proxystore.proxy import Proxy
from proxystore.store import register_store
from proxystore.store import Store
from proxystore.connectors.mictlan import MictlanConnector

from mictlanx.utils.index import Utils
import os

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

store = Store('my-store', conn)
data = 123
proxy = store.proxy(data)
print(proxy ** 2)