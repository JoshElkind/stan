import requests

API_URL = "http://3.83.214.56/api/scripts/user/"
ACCESS_TOKEN = "your_access_token_here"

headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

try:
    response = requests.get(API_URL, headers=headers)
    print("Status Code:", response.status_code)
    print("Raw Response:", response.text)  # <-- This shows what actually came back
    # Optional: try to parse JSON after inspecting text
    try:
        print("JSON:", response.json())
    except Exception as e:
        print("❌ Failed to parse JSON:", e)
except requests.exceptions.RequestException as e:
    print("❌ Request failed:", e)
