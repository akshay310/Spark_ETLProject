
import requests

url = "https://data.cityofnewyork.us/api/views/pkmi-4kfn/rows.json?accessType=DOWNLOAD"
response = requests.get(url)

if response.status_code == 200:
    with open("newyorkcity_taxi.json", "wb") as file:
        file.write(response.content)
    print("Download complete!")
else:
    print("Failed to download file:", response.status_code)