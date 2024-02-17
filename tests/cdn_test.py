from proxystore.proxy import Proxy
from proxystore.store import register_store
from proxystore.store import Store
from proxystore.connectors.cdn import CDNConnector
import time

conn = CDNConnector("68b97d2a3a9e9b51416e82cc5b31925df6ebab307157f1a2cb8f915f1004acd4")

start = time.time()
print(
    conn.put(
        data=b'hello world', 
        filepath=None, 
        is_encrypted=False, 
        number_of_chunks=2, 
        required_chunks=1, 
        disperse="IDA", 
        parallel=True,
        workers=1
    )
)

end = time.time()

print(end - start)

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
