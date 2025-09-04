import requests
import psycopg2
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from dotenv import load_dotenv
import os


# Load the .env file
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}


APIS = [
    {
        "name": "AWS",
        "url": os.getenv("AWS_URL"),
        "table": os.getenv("AWS_TABLE"),
        "username": os.getenv("AWS_USERNAME"),
        "password": os.getenv("AWS_PASSWORD"),
        "fields": [
            "inputDate","batteryLevel","hourlyRainFall","dailyRainfall",
            "averageTempreture","windSpeed","windDirection","atmosphericPressure",
            "relativeHumidity","sunRadiation","stationID"
        ]
    },
    {
        "name": "AWLR",
        "url": os.getenv("AWLR_URL"),
        "table": os.getenv("AWLR_TABLE"),
        "username": os.getenv("AWLR_USERNAME"),
        "password": os.getenv("AWLR_PASSWORD"),
        "fields": [
            "inputDate","batteryLevel","waterLevel","stationID"
        ]
    },
    {
        "name": "ARG",
        "url": os.getenv("ARG_URL"),
        "table": os.getenv("ARG_TABLE"),
        "username": os.getenv("ARG_USERNAME"),
        "password": os.getenv("ARG_PASSWORD"),
        "fields": [
            "inputDate","batteryLevel","hourlyRainFall","dailyRainfall","stationID"
        ]
    }
]

# === CONNECT DATABASE ===
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

# === FETCH DATA FROM API ===
def fetch_data(api):
    resp = requests.get(api["url"], auth=(api["username"], api["password"]))
    resp.raise_for_status()
    return resp.json().get("content", [])

# === CLEAN RECORD ===
def clean_record(record, fields):
    filtered = {"uuid": str(uuid.uuid4())}
    for f in fields:
        val = record.get(f)
        if val in [-99, None, ""]:  # skip faulty
            return None
        filtered[f] = val
    return filtered

# === INSERT DATA ===
def insert_data(api, records):
    if not records:
        return 0
    conn = get_connection()
    cur = conn.cursor()
    
    cols = ["uuid"] + api["fields"]
    placeholders = ",".join(["%s"] * len(cols))
    colnames = ",".join(cols)

    for rec in records:
        try:
            cur.execute(f"""
                INSERT INTO {api['table']} ({colnames})
                VALUES ({placeholders})
                ON CONFLICT (stationID, inputDate) DO NOTHING;
            """, tuple(rec[c] for c in cols))
        except Exception as e:
            print(f"⚠️ {api['name']} - Error inserting record: {e}")

    conn.commit()
    cur.close()
    conn.close()
    return len(records)

# === RUN PIPELINE FOR ONE API ===
def process_api(api):
    try:
        raw = fetch_data(api)
        cleaned = [clean_record(r, api["fields"]) for r in raw]
        cleaned = [r for r in cleaned if r]
        inserted = insert_data(api, cleaned)
        return f"✅ {api['name']} inserted {inserted} records"
    except Exception as e:
        return f"❌ {api['name']} failed: {e}"

# === MAIN ===
if __name__ == "__main__":
    results = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(process_api, api) for api in APIS]
        for f in as_completed(futures):
            results.append(f.result())
    
    print(f"\nRun finished @ {datetime.now()}")
    for r in results:
        print(r)
