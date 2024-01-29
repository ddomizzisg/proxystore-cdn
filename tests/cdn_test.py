from proxystore.proxy import Proxy
from proxystore.store import register_store
from proxystore.store import Store
from proxystore.connectors.cdn import CDNConnector

conn = CDNConnector("5d2fc6edda606f65f53ad51062fb681332f2ad192c419db816e2712b3edbd008")

store = Store('my-store', conn)

# Store the object and get a proxy. The proxy acts
# like a reference to the object.
data = 123
proxy = store.proxy(data)
print(proxy ** 2)
#print(isinstance(proxy, Proxy))

def my_function(x: int) -> ...:
    # x is resolved my-store on first use transparently to the
    # function. Then x behaves as an instance of int.
    print(x + 1)
    print( isinstance(x, int))

#my_function(proxy)  # Succeeds"""
