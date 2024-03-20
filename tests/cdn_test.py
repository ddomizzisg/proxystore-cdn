from proxystore.proxy import Proxy
from proxystore.store import register_store
from proxystore.store import Store
from proxystore.connectors.cdn import CDNConnector
from proxystore.connectors.cdn import CDNKey
import time
import math
from bechmark.cdn import utils

import proxystore.cdn.client as client


conn = CDNConnector("dynostore")

data = utils.randbytes(100000)
data = b'bytes(500)'

key = CDNKey(cdn_key="4e7b8448-2835-44c6-8312-4c5a8670317c")
print(conn.exists(key))
key,time = conn.put(
           data=data, 
           filepath=None, 
           is_encrypted=False
       )
print(key)
print(conn.exists(key))
#print(conn.get(key))
#conn.evict(key)
#print(conn.get(key))

#times = []
#for i in range(1):

#    start = time.time()
#    print(        
#        conn.put(
#            data=data, 
#            filepath=None, 
#            is_encrypted=False
#        )
#    )
#    end = time.time()
#    times.append(end - start)
    
#print(math.fsum(times) / len(times))

#store = Store('my-store', conn)

# Store the object and get a proxy. The proxy acts
# like a reference to the object.
#data = 123
#proxy = store.proxy(data)
#print(proxy ** 2)
#print(isinstance(proxy, Proxy))

#def my_function(x: int) -> ...:
    # x is resolved my-store on first use transparently to the
    # function. Then x behaves as an instance of int.
#    print(x + 1)
#    print( isinstance(x, int))

#my_function(proxy)  # Succeeds"""
