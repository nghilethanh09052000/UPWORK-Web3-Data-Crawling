import requests

url = "https://apis.dappradar.com/v2//dapps/chains"

response = requests.get(url)

print(response.json())