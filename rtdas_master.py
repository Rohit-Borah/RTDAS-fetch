import requests
import psycopg2
from dotenv import load_dotenv
import os

# Load the .env file
load_dotenv()


# === DB CONFIG ===
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# === API CONFIGS (Station Master Data Endpoints) ===
APIS = [
    {
        "name": "AWS Master",
        "url": os.getenv("AWS_MASTER_URL"),
        "username": os.getenv("AWS_MASTER_USERNAME"),
        "password": os.getenv("AWS_MASTER_PASSWORD"),
        "source" : "AWS station"
    },
    {
        "name": "AWLR Master",
        "url": os.getenv("AWLR_MASTER_URL"),
        "username": os.getenv("AWLR_MASTER_USERNAME"),
        "password": os.getenv("AWLR_MASTER_PASSWORD"),
        "source" : "AWLR station"
    },
    {
        "name": "ARG Master",
        "url": os.getenv("ARG_MASTER_URL"),
        "username": os.getenv("ARG_MASTER_USERNAME"),
        "password": os.getenv("ARG_MASTER_PASSWORD"),
        "source" : "ARG station"
    }
]

# === CONNECT DB ===
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

# === FETCH DATA ===
def fetch_data(api):
    resp = requests.get(api["url"], auth=(api["username"], api["password"]))  
    resp.raise_for_status()
    return resp.json().get("data", [])

# === UPSERT INTO MASTER ===
def upsert_data(api, records):
    if not records:
        return 0
    
    conn = get_connection()
    cur = conn.cursor()
    
    for r in records:
        try:
            cur.execute(f"""
                INSERT INTO rtdas_master
                (stationID, name, location, longitude, latitude, type, zone, source)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (stationID) DO UPDATE SET
                  name = EXCLUDED.name,
                  location = EXCLUDED.location,
                  longitude = EXCLUDED.longitude,
                  latitude = EXCLUDED.latitude,
                  type = EXCLUDED.type,
                  zone = EXCLUDED.zone,
                  source = EXCLUDED.source;
            """, (
                r.get("stationID"),
                r.get("name"),
                r.get("location"),
                r.get("longitude"),
                r.get("latitude"),
                r.get("type"),
                r.get("zone"),
                api["source"]
            ))
        except Exception as e:
            print(f"⚠️ Error inserting {r.get('stationID')}: {e}")
    
    conn.commit()
    cur.close()
    conn.close()
    return len(records)

# === MAIN ===
if __name__ == "__main__":
    for api in APIS:
        print(f"⏳ Fetching {api['name']}...")
        data = fetch_data(api)
        inserted = upsert_data(api, data)
        print(f"✅ {api['name']} updated {inserted} stations.")
