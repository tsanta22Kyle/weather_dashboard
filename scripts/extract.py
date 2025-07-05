import requests
import psycopg2 as pg
from datetime import datetime
import calendar
import numpy as np
import logging

def extract_realtime_data():
    cities = [
        {"q": "Bogota", "latitude": 4.7110, "longitude": -74.0721},
        {"q": "Antananarivo", "latitude": -18.8792, "longitude": 47.5079},
        {"q": "Taipei", "latitude": 25.0330, "longitude": 121.5654},
        {"q": "Kiev", "latitude": 50.4501, "longitude": 30.5234},
        {"q": "Paris", "latitude": 48.8566, "longitude": 2.3522},
        {"q": "Munich", "latitude": 48.1351, "longitude": 11.5820},
        {"q": "Tokyo", "latitude": 35.6895, "longitude": 139.6917},
        {"q": "Venise", "latitude": 45.4408, "longitude": 12.3155},
        {"q": "Rio de Janeiro", "latitude": -22.9068, "longitude": -43.1729},
    ]

    exec_date = datetime.today().strftime("%Y-%m-%d")

    for city in cities:
        try:
            conn = pg.connect(
                dbname="meteo_db",
                user="airflow",
                password="tsanta",
                host="localhost",
                port="5432"
            )
            cur = conn.cursor()

            name = city["q"]
            lat = city["latitude"]
            lon = city["longitude"]

            response = requests.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": lat,
                    "longitude": lon,
                    "daily": "sunshine_duration",
                    "hourly": ["temperature_2m", "wind_speed_10m", "precipitation", "relative_humidity_2m"],
                    "timezone": "auto"
                }
            )
            data = response.json()

            sun_seconds = data.get("daily", {}).get("sunshine_duration", [0])[0] or 0
            sun_hours = round(sun_seconds / 3600, 2)

            temps = data.get("hourly", {}).get("temperature_2m", [])
            winds = data.get("hourly", {}).get("wind_speed_10m", [])
            humids = data.get("hourly", {}).get("relative_humidity_2m", [])
            precips = data.get("hourly", {}).get("precipitation", [])

            temp_max = max([t for t in temps if t is not None], default=0)
            wind_avg = float(round(np.mean([w for w in winds if w is not None]) if winds else 0, 2))
            humid_avg = float(round(np.mean([h for h in humids if h is not None]) if humids else 0, 2))
            precip_sum = float(round(sum([p for p in precips if p is not None]) if precips else 0, 2))

            # Score météo safe
            score = 0
            if 22 <= temp_max <= 28: score += 3
            elif 18 <= temp_max < 22 or 28 < temp_max <= 32: score += 2
            elif 15 <= temp_max < 18 or 32 < temp_max <= 35: score += 1
            if precip_sum < 1: score += 2
            elif precip_sum < 5: score += 1
            if 40 <= humid_avg <= 70: score += 1
            if wind_avg < 20: score += 1
            if sun_hours > 4: score += 1

            weather_score = score

            full_date = datetime.strptime(exec_date, "%Y-%m-%d")
            date_id = int(full_date.strftime("%Y%m%d"))
            day = full_date.day
            month = full_date.month
            year = full_date.year
            month_name = calendar.month_name[month]
            season = (
                "Spring" if month in [3, 4, 5]
                else "Summer" if month in [6, 7, 8]
                else "Autumn" if month in [9, 10, 11]
                else "Winter"
            )

            cur.execute("SELECT city_id FROM dim_city WHERE city_name = %s", (name,))
            city_row = cur.fetchone()
            if city_row:
                city_id = city_row[0]
            else:
                cur.execute("""
                    INSERT INTO dim_city (city_name, country, continent, latitude, longitude)
                    VALUES (%s, %s, %s, %s, %s) RETURNING city_id
                """, (name, None, None, lat, lon))
                city_id = cur.fetchone()[0]

            cur.execute("""
                INSERT INTO dim_date (date_id, full_date, day, month, month_name, year, season)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date_id) DO NOTHING
            """, (date_id, full_date, day, month, month_name, year, season))

            cur.execute("""
                INSERT INTO weather_fact (
                    date_id, city_id, temp_max, precipitation,
                    wind_speed, humidity, sun_hours, 
                    weather_score, source
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                date_id, city_id, temp_max, precip_sum,
                wind_avg, humid_avg, sun_hours,
                weather_score, "realtime"
            ))

            conn.commit()
            logging.info(f"✅ Données météo insérées pour {name} ({exec_date})")

        except Exception as e:
            logging.error(f"❌ Erreur pour {name} ({exec_date}) : {e}")
        finally:
            if cur: cur.close()
            if conn: conn.close()
