from proxystore.connectors.file import FileConnector
from proxystore.proxy import Proxy
from proxystore.store import register_store
from proxystore.store import Store
from proxystore.connectors.cdn import CDNConnector

conn = CDNConnector("4cd374d24ce9398755d8aae5bce7c3ce96957eca8ea12e10c23dac309bcfc222")

store = Store('my-store', conn)

# Store the object and get a proxy. The proxy acts
# like a reference to the object.
data = 123
proxy = store.proxy(data)
print(proxy ** 2)
print(isinstance(proxy, Proxy))

def my_function(x: int) -> ...:
    # x is resolved my-store on first use transparently to the
    # function. Then x behaves as an instance of int.
    print(x + 1)
    print( isinstance(x, int))

my_function(proxy)  # Succeeds"""
