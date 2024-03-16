import requests


response = requests.put(url = "http://localhost:20001/objects/12345/1234", data = "Hello, World!")

print(response.text)

response = requests.get(url = "http://localhost:20001/objects/12345/1234", data = "Hello, World!")

print(response.text)

#response = requests.delete(url = "http://localhost:20001/objects/12345/1234", data = "Hello, World!")

print(response.text)