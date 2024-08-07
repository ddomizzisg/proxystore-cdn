from proxystore.proxy import Proxy
from proxystore.store import register_store
from proxystore.store import Store
from proxystore.connectors.cdn import CDNConnector
from proxystore.connectors.cdn import CDNKey
import time
import math

# Store the object and get a proxy. The proxy acts
# like a reference to the object.
store = Store('cdn', CDNConnector(catalog="proxystore"))
data = 'Hello World'

data = store.proxy(data)
#key, time = connector.put(data)
print(data + "a")
#print(connector.exists(key))
#connector.evict(key)
#print(connector.exists(key))

#store = Store('my-store', connector)
#data = 3
#proxy = store.proxy(data)
#print(proxy ** 2)
#print(isinstance(proxy, Proxy))

#def my_function(x: int) -> ...:
    # x is resolved my-store on first use transparently to the
    # function. Then x behaves as an instance of int.
#    print(x + 1)
#    print( isinstance(x, int))

#my_function(proxy)  # Succeeds"""
