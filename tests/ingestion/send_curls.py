from utils import load_fhir_test_data
import requests

#1. Run docker compose up
#2. Send post requests of all data in the test folder
data = load_fhir_test_data()

for entry in data:
    
    with open(entry["path"], "rb") as f:
        payload = f.read()

    response = requests.post(
        entry["url"],
        headers={"Content-Type": f"application/{entry['ext']}"},
        data=payload,
    )

    print("Status:", response.status_code)
    print("Response:", response.text)
    print("Headers:", response.headers)
