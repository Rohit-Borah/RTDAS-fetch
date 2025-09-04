import requests
import psycopg2
import uuid
from datetime import datetime
from dotenv import load_dotenv
import os

# Load the .env file
load_dotenv()

# === API CONFIG ===
API_URL = os.getenv("AWS_URL")
USERNAME = os.getenv("AWS_USERNAME")
PASSWORD = os.getenv("AWS_PASSWORD")

# === DATABASE CONFIG ===
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

TABLE_NAME = os.getenv("AWS_TABLE")

# === CONNECT DATABASE ===
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

# === FETCH DATA FROM API ===
def fetch_data():
    response = requests.get(API_URL, auth=(USERNAME, PASSWORD))
    response.raise_for_status()
    return response.json().get("content", [])

# === CLEAN & FILTER DATA ===
def clean_record(record):
    # Exclude unwanted fields
    filtered = {
        "uuid": str(uuid.uuid4()),
        "inputDate": record.get("inputDate"),
        "batteryLevel": record.get("batteryLevel"),
        "hourlyRainFall": record.get("hourlyRainFall"),
        "dailyRainfall": record.get("dailyRainfall"),
        "averageTempreture": record.get("averageTempreture"),
        "windSpeed": record.get("windSpeed"),
        "windDirection": record.get("windDirection"),
        "atmosphericPressure": record.get("atmosphericPressure"),
        "relativeHumidity": record.get("relativeHumidity"),
        "sunRadiation": record.get("sunRadiation"),
        "stationID": record.get("stationID")
    }

    # Skip faulty data (-99 or None values)
    for k, v in filtered.items():
        if v in [-99, None, ""]:
            return None
    return filtered

# === INSERT DATA INTO PSQL (avoid duplicates) ===
def insert_data(records):
    if not records:
        return

    conn = get_connection()
    cur = conn.cursor()

    for rec in records:
        try:
            cur.execute(f"""
                INSERT INTO {TABLE_NAME} 
                (uuid, inputDate, batteryLevel, hourlyRainFall, dailyRainfall, 
                 averageTempreture, windSpeed, windDirection, atmosphericPressure, 
                 relativeHumidity, sunRadiation, stationID)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (stationID, inputDate) DO NOTHING;
            """, (
                rec["uuid"], rec["inputDate"], rec["batteryLevel"], rec["hourlyRainFall"],
                rec["dailyRainfall"], rec["averageTempreture"], rec["windSpeed"],
                rec["windDirection"], rec["atmosphericPressure"], rec["relativeHumidity"],
                rec["sunRadiation"], rec["stationID"]
            ))
        except Exception as e:
            print(f"⚠️ Skipping record {rec['stationID']} @ {rec['inputDate']} due to error: {e}")

    conn.commit()
    cur.close()
    conn.close()

# === MAIN ===
if __name__ == "__main__":
    raw_data = fetch_data()
    cleaned = [clean_record(r) for r in raw_data]
    cleaned = [r for r in cleaned if r]  # remove None
    insert_data(cleaned)
    print(f"✅ Inserted {len(cleaned)} records at {datetime.now()}")
