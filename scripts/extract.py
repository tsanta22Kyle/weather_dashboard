import requests
import psycopg2 as pg
from datetime import datetime
import calendar
import numpy as np
import logging

def extract_forecast_week():
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

    conn = pg.connect(
        dbname="meteo_db",
        user="airflow",
        password="tsanta",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    for city in cities:
        try:
            name = city["q"]
            lat = city["latitude"]
            lon = city["longitude"]

            response = requests.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": lat,
                    "longitude": lon,
                    "daily": "temperature_2m_max,precipitation_sum,sunshine_duration,wind_speed_10m_max,uv_index_max",
                    "timezone": "auto"
                }
            )
            data = response.json()

            # Récupérer ou créer city_id
            cur.execute("SELECT city_id FROM dim_city WHERE city_name = %s", (name,))
            row = cur.fetchone()
            if row:
                city_id = row[0]
            else:
                cur.execute("""
                    INSERT INTO dim_city (city_name, country, continent, latitude, longitude)
                    VALUES (%s, %s, %s, %s, %s) RETURNING city_id
                """, (name, None, None, lat, lon))
                city_id = cur.fetchone()[0]

            # Récupérer les prévisions journalières
            daily_times = data.get("daily", {}).get("time", [])
            temps_max = data.get("daily", {}).get("temperature_2m_max", [])
            precips = data.get("daily", {}).get("precipitation_sum", [])
            suns = data.get("daily", {}).get("sunshine_duration", [])
            winds = data.get("daily", {}).get("wind_speed_10m_max", [])
            uvs = data.get("daily", {}).get("uv_index_max", [])

            for i in range(len(daily_times)):
                date_str = daily_times[i]
                full_date = datetime.strptime(date_str, "%Y-%m-%d")
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

                temp = temps_max[i] or 0
                precip = round(precips[i] or 0, 2)
                sun_hours = round((suns[i] or 0) / 3600, 2)
                wind = round(winds[i] or 0, 2)
                uv = uvs[i] or None

                # Calcul du score météo
                score = 0
                if 22 <= temp <= 28: score += 3
                elif 18 <= temp < 22 or 28 < temp <= 32: score += 2
                elif 15 <= temp < 18 or 32 < temp <= 35: score += 1
                if precip < 1: score += 2
                elif precip < 5: score += 1
                if wind < 20: score += 1
                if sun_hours > 4: score += 1

                weather_score = score

                # Upsert dim_date
                cur.execute("""
                    INSERT INTO dim_date (date_id, full_date, day, month, month_name, year, season)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date_id) DO NOTHING
                """, (date_id, full_date, day, month, month_name, year, season))

                # Upsert weather_fact
                cur.execute("""
                    INSERT INTO weather_fact (
                        date_id, city_id, temp_max, precipitation,
                        wind_speed, humidity, sun_hours,
                        weather_score, source
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (date_id, city_id, source)
                    DO UPDATE SET
                        temp_max = EXCLUDED.temp_max,
                        precipitation = EXCLUDED.precipitation,
                        wind_speed = EXCLUDED.wind_speed,
                        sun_hours = EXCLUDED.sun_hours,
                        weather_score = EXCLUDED.weather_score
                """, (
                    date_id, city_id, temp, precip,
                    wind, None, sun_hours,
                    weather_score, "forecast"
                ))

            conn.commit()
            logging.info(f"✅ Prévisions insérées pour {name}")

        except Exception as e:
            logging.error(f"❌ Erreur pour {name} : {e}")
            conn.rollback()

    cur.close()
    conn.close()
