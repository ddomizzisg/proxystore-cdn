from proxystore.cdn.client import Client
import uuid

token_user = "user-0"
catalog = "catalog-0"
data = b"Hello, World!"


client = Client("metadata_server")

client.put(
    data = data,
    token_user = token_user,
    catalog = catalog
)

received_data = client.get(
    key = key,
    token_user = token_user
)