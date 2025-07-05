import requests
import psycopg2 as pg
from datetime import datetime, date, timedelta
import calendar
import numpy as np
import logging

def extract_historical_data():
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
        name = city["q"]
        lat = city["latitude"]
        lon = city["longitude"]

        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": "2024-06-01",
            "end_date": "2025-07-03",
            "daily": "sunshine_duration",
            "hourly": [
                "temperature_2m",
                "wind_speed_10m",
                "precipitation",
                "relative_humidity_2m"
            ],
            "timezone": "auto"
        }

        response = requests.get("https://archive-api.open-meteo.com/v1/archive", params=params)
        data = response.json()

        try:
            daily_dates = data["daily"]["time"]
            daily_sun = data["daily"].get("sunshine_duration", [0] * len(daily_dates))
            sun_by_date = dict(zip(daily_dates, daily_sun))

            times = data["hourly"]["time"]
            temp = data["hourly"]["temperature_2m"]
            wind = data["hourly"]["wind_speed_10m"]
            humid = data["hourly"]["relative_humidity_2m"]
            precip = data["hourly"]["precipitation"]

            daily_data = {}
            for t, t2m, w10m, h2m, p in zip(times, temp, wind, humid, precip):
                date_str = t.split("T")[0]
                if date_str not in daily_data:
                    daily_data[date_str] = {
                        "temps": [],
                        "winds": [],
                        "humidity": [],
                        "precip": [],
                    }
                daily_data[date_str]["temps"].append(t2m)
                daily_data[date_str]["winds"].append(w10m)
                daily_data[date_str]["humidity"].append(h2m)
                daily_data[date_str]["precip"].append(p)

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

            for date_str, values in daily_data.items():
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

                # Nettoyage des listes
                temps_list = [v for v in values["temps"] if v is not None]
                winds_list = [v for v in values["winds"] if v is not None]
                humidity_list = [v for v in values["humidity"] if v is not None]
                precip_list = [v for v in values["precip"] if v is not None]

                temp_max = max(temps_list) if temps_list else None
                wind_avg = float(round(np.mean(winds_list), 2)) if winds_list else None
                humid_avg = float(round(np.mean(humidity_list), 2)) if humidity_list else None
                precip_sum = float(round(sum(precip_list), 2)) if precip_list else None
                sun_hours = float(round((sun_by_date.get(date_str, 0) or 0) / 3600, 2))

                # Skip si données manquantes
                if temp_max is None or wind_avg is None or humid_avg is None or precip_sum is None:
                    continue

                score = 0
                if 22 <= temp_max <= 28:
                    score += 3
                elif 18 <= temp_max < 22 or 28 < temp_max <= 32:
                    score += 2
                elif 15 <= temp_max < 18 or 32 < temp_max <= 35:
                    score += 1

                if precip_sum < 1:
                    score += 2
                elif precip_sum < 5:
                    score += 1

                if 40 <= humid_avg <= 70:
                    score += 1

                if wind_avg < 20:
                    score += 1

                if sun_hours > 4:
                    score += 1

                weather_score = score

                cur.execute("""
                    INSERT INTO dim_date (date_id, full_date, day, month, month_name, year, season)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date_id) DO NOTHING
                """, (date_id, full_date, day, month, month_name, year, season))

                cur.execute("""
                    INSERT INTO weather_fact (
                        date_id, city_id, temp_max, precipitation,
                        wind_speed, humidity, sun_hours, uv_index,
                        weather_score, source
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    date_id, city_id, temp_max, precip_sum,
                    wind_avg, humid_avg, sun_hours, None,
                    weather_score, "historical"
                ))

            conn.commit()
            logging.info(f"✅ Données insérées pour {name}")

        except Exception as e:
            logging.error(f"❌ Erreur d'insertion pour {name} : {e}")
            continue

    cur.close()
    conn.close()
    logging.info("✅ Extraction terminée pour toutes les villes.")

