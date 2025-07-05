import psycopg2 as pg
from collections import defaultdict

# ✅ Fonction safe_avg déplacée en dehors
def safe_avg(lst):
    return round(sum(lst) / len(lst), 2) if lst else None

def transform_weather():
    conn = pg.connect(
        dbname="meteo_db",
        user="airflow",
        password="tsanta",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    cur.execute("""
        SELECT
            wf.city_id,
            dd.month,
            dd.month_name,
            dd.season,
            dd.year,
            wf.temp_max,
            wf.precipitation,
            wf.wind_speed,
            wf.humidity,
            wf.sun_hours,
            wf.weather_score,
            wf.source
        FROM weather_fact wf
        JOIN dim_date dd ON wf.date_id = dd.date_id
    """)
    
    rows = cur.fetchall()

    grouped = defaultdict(list)
    for row in rows:
        key = (row[0], row[1], row[4], row[11])  # city_id, month, year, source
        grouped[key].append(row)

    for (city_id, month, year, source), records in grouped.items():
        temps = [r[5] for r in records if r[5] is not None]
        precips = [r[6] for r in records if r[6] is not None]
        winds = [r[7] for r in records if r[7] is not None]
        humids = [r[8] for r in records if r[8] is not None]
        suns = [r[9] for r in records if r[9] is not None]
        scores = [r[10] for r in records if r[10] is not None]
        month_name = records[0][2]
        season = records[0][3]

        avg_temp = safe_avg(temps)
        avg_precip = safe_avg(precips)
        avg_humid = safe_avg(humids)
        avg_wind = safe_avg(winds)
        avg_sun = safe_avg(suns)
        avg_score = safe_avg(scores)

        cur.execute("""
            INSERT INTO city_weather_summary (
                city_id, month, month_name, season, year,
                avg_temp, avg_precipitation, avg_humidity,
                avg_wind, avg_sun_hours, avg_score, source
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (city_id, month, year, source)
            DO UPDATE SET
                avg_temp = EXCLUDED.avg_temp,
                avg_precipitation = EXCLUDED.avg_precipitation,
                avg_humidity = EXCLUDED.avg_humidity,
                avg_wind = EXCLUDED.avg_wind,
                avg_sun_hours = EXCLUDED.avg_sun_hours,
                avg_score = EXCLUDED.avg_score;
        """, (
            city_id, month, month_name, season, year,
            avg_temp, avg_precip, avg_humid, avg_wind, avg_sun, avg_score, source
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Agrégation terminée sans doublon.")

