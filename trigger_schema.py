import requests

try:
    response = requests.post("http://0.0.0.0:8000/api/apply-schema")
    print(f"Status: {response.status_code}")
    print(response.json())
except Exception as e:
    print(f"Error: {e}")
