import requests
import json
import os

def request_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Gây lỗi nếu status_code >= 400
        data = response.json()
    except requests.exceptions.RequestException as e:
        return f"Request failed: {e}"
    
    raw_dir = '/usr/local/airflow/data/raw'
    os.makedirs(raw_dir, exist_ok=True)  # Tạo thư mục nếu chưa có

    path = os.path.join(raw_dir, 'raw.json')

    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=4)
        print(f"✅ Data saved to {path}")
        return f"Success: saved {len(data)} records."
    except Exception as e:
        return f"Failed to write data to file: {e}"

# if __name__ == "__main__":
#     url = "https://restcountries.com/v3.1/independent?status=true"
#     print(request_data(url))
