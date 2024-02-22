from proxystore.proxy import Proxy
from proxystore.store import register_store
from proxystore.store import Store
from proxystore.connectors.cdn import CDNConnector
import time
import math
from bechmark.cdn import utils

conn = CDNConnector("68b97d2a3a9e9b51416e82cc5b31925df6ebab307157f1a2cb8f915f1004acd4")

data = utils.randbytes(100000)

times = []
for i in range(1):

    start = time.time()
    print(
        #conn.put(
        ##    data=data, 
        #    filepath=None, 
        #    is_encrypted=False, 
        #    number_of_chunks=5, 
        #    required_chunks=3, 
        #    disperse="IDA", 
        #    workers=5
        #)
        
        conn.put(
            data=data, 
            filepath=None, 
            is_encrypted=False
        )
    )
    end = time.time()
    times.append(end - start)
    
print(math.fsum(times) / len(times))

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
