#!/usr/bin/env python3
"""
Generate sample data for the Bike-Share Operations Platform.

This script creates realistic synthetic data that mirrors the production
BigQuery schema, allowing local development and demonstration without
access to the real data warehouse.

Usage:
    python seed_data.py                    # Generate all sample data
    python seed_data.py --output ./data    # Custom output directory
    python seed_data.py --days 30          # Generate 30 days of data
"""
import os
import argparse
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

# --- Configuration ---

CENTERS = [
    {"id": 1, "name": "Center_North", "lat": 37.66, "lng": 126.77},
    {"id": 2, "name": "Center_West", "lat": 37.49, "lng": 126.86},
    {"id": 3, "name": "Center_South", "lat": 37.34, "lng": 126.94},
    {"id": 4, "name": "Center_East", "lat": 37.59, "lng": 127.17},
    {"id": 5, "name": "Center_Central", "lat": 36.48, "lng": 127.00},
    {"id": 6, "name": "Partner_Seoul", "lat": 37.55, "lng": 126.97},
    {"id": 7, "name": "Partner_Daejeon", "lat": 36.35, "lng": 127.38},
    {"id": 8, "name": "Partner_Gwacheon", "lat": 37.43, "lng": 126.99},
    {"id": 9, "name": "Partner_Ansan", "lat": 37.32, "lng": 126.83},
]

REGIONS = [
    {"name": "Region_A1", "center_id": 1, "lat": 37.67, "lng": 126.78},
    {"name": "Region_A2", "center_id": 1, "lat": 37.65, "lng": 126.76},
    {"name": "Region_B1", "center_id": 2, "lat": 37.50, "lng": 126.87},
    {"name": "Region_B2", "center_id": 2, "lat": 37.48, "lng": 126.85},
    {"name": "Region_C1", "center_id": 3, "lat": 37.35, "lng": 126.95},
    {"name": "Region_D1", "center_id": 4, "lat": 37.60, "lng": 127.18},
    {"name": "Region_E1", "center_id": 5, "lat": 36.49, "lng": 127.01},
    {"name": "Region_F1", "center_id": 6, "lat": 37.56, "lng": 126.98},
    {"name": "Region_G1", "center_id": 7, "lat": 36.36, "lng": 127.39},
    {"name": "Region_H1", "center_id": 8, "lat": 37.44, "lng": 127.00},
    {"name": "Region_I1", "center_id": 9, "lat": 37.33, "lng": 126.84},
]

DISTRICTS_PER_REGION = 5

BIKE_STATUSES = ["BAV", "BNB", "BRD", "LRD", "LAV", "LNB", "BB", "LB", "BNP", "LNP", "BP", "LP"]
BIKE_STATUS_WEIGHTS = [40, 10, 8, 2, 15, 5, 3, 2, 5, 3, 4, 3]

HOLIDAYS = [
    "2025-01-01", "2025-01-28", "2025-01-29", "2025-01-30",
    "2025-03-01", "2025-05-05", "2025-05-06", "2025-06-06",
    "2025-08-15", "2025-10-03", "2025-10-05", "2025-10-06",
    "2025-10-07", "2025-10-09", "2025-12-25",
]


def generate_districts():
    """Generate district-level geographic data."""
    districts = []
    for region in REGIONS:
        for i in range(DISTRICTS_PER_REGION):
            districts.append({
                "region": region["name"],
                "district": f"{region['name']}_D{i+1:02d}",
                "center_id": region["center_id"],
                "lat": region["lat"] + np.random.normal(0, 0.01),
                "lng": region["lng"] + np.random.normal(0, 0.01),
            })
    return pd.DataFrame(districts)


def generate_bikes(n_bikes=2000):
    """Generate bike fleet data."""
    bikes = []
    for i in range(n_bikes):
        center = random.choice(CENTERS)
        region = random.choice([r for r in REGIONS if r["center_id"] == center["id"]] or REGIONS)
        bikes.append({
            "bike_id": 10000 + i,
            "sn": f"BK{10000 + i}",
            "type": random.choices([1, 2], weights=[80, 20])[0],  # 1=bike, 2=scooter
            "vendor": random.choice([0, 1, 2, 3]),
            "center_id": center["id"],
            "region": region["name"],
            "is_active": random.random() > 0.05,
        })
    return pd.DataFrame(bikes)


def generate_bike_snapshot(bikes_df, dates, hours=range(24)):
    """Generate hourly bike status snapshots."""
    rows = []
    for date in dates:
        is_weekend = date.weekday() >= 5
        for hour in hours:
            for _, bike in bikes_df.iterrows():
                if not bike["is_active"]:
                    continue
                if random.random() < 0.3:  # not all bikes appear every hour
                    continue

                status = random.choices(BIKE_STATUSES, weights=BIKE_STATUS_WEIGHTS)[0]
                battery = max(0, min(100, int(np.random.normal(60, 25))))
                if status.startswith("L"):
                    battery = min(battery, 25)

                region_info = next((r for r in REGIONS if r["name"] == bike["region"]), REGIONS[0])
                lat = region_info["lat"] + np.random.normal(0, 0.005)
                lng = region_info["lng"] + np.random.normal(0, 0.005)

                rows.append({
                    "date": date,
                    "hour": hour,
                    "time": datetime.combine(date, datetime.min.time()).replace(hour=hour),
                    "bike_id": bike["bike_id"],
                    "sn": bike["sn"],
                    "type": bike["type"],
                    "vendor": bike["vendor"],
                    "bike_status": status,
                    "leftover": battery,
                    "battery": battery,
                    "is_active": True,
                    "is_usable": status in ("BAV", "BNB"),
                    "is_chargeable": battery < 30,
                    "h3_area_name": bike["region"],
                    "center_id": bike["center_id"],
                    "lat": lat,
                    "lng": lng,
                })
    return pd.DataFrame(rows)


def generate_app_accessibility(districts_df, dates):
    """Generate app accessibility (app open) events."""
    rows = []
    event_id = 0
    for date in dates:
        is_weekend = date.weekday() >= 5
        is_holiday = date.strftime("%Y-%m-%d") in HOLIDAYS

        for hour in range(6, 23):
            # Demand varies by hour (peaks at 8, 12, 18)
            hour_factor = 1.0
            if hour in (7, 8, 9):
                hour_factor = 2.0
            elif hour in (12, 13):
                hour_factor = 1.5
            elif hour in (17, 18, 19):
                hour_factor = 2.2
            elif hour >= 21:
                hour_factor = 0.5

            if is_weekend or is_holiday:
                hour_factor *= 0.7

            for _, district in districts_df.iterrows():
                n_events = max(0, int(np.random.poisson(3 * hour_factor)))
                for _ in range(n_events):
                    event_id += 1
                    bike_count_100 = max(0, int(np.random.poisson(2)))
                    bike_count_400 = bike_count_100 + max(0, int(np.random.poisson(3)))
                    is_accessible = bike_count_100 > 0
                    distance = random.randint(10, 800) if is_accessible else random.randint(200, 2000)
                    is_converted = is_accessible and random.random() < (0.4 + 0.1 * min(bike_count_100, 5))

                    rows.append({
                        "event_id": event_id,
                        "date": date,
                        "hour": hour,
                        "event_time": datetime.combine(date, datetime.min.time()).replace(
                            hour=hour, minute=random.randint(0, 59)),
                        "service_group_name": "Metro",
                        "h3_area_name": district["region"],
                        "h3_district_name": district["district"],
                        "bike_count_100": bike_count_100,
                        "bike_count_400": bike_count_400,
                        "distance": distance,
                        "is_accessible": is_accessible,
                        "is_converted": is_converted,
                        "user_id": random.randint(1000, 50000),
                        "near_geoblock": random.random() < 0.05,
                    })
    return pd.DataFrame(rows)


def generate_rides(accessibility_df, dates):
    """Generate ride records from converted app opens."""
    converted = accessibility_df[accessibility_df["is_converted"]].copy()
    rows = []
    for _, event in converted.iterrows():
        duration = max(3, int(np.random.lognormal(2.5, 0.6)))  # minutes
        distance_m = max(100, int(duration * np.random.normal(180, 50)))
        fee = max(0, int(duration * np.random.normal(15, 3)))

        district_info = next(
            (d for d in REGIONS if d["name"] == event["h3_area_name"]),
            {"lat": 37.5, "lng": 127.0, "center_id": 1}
        )

        rows.append({
            "start_time": event["event_time"],
            "end_time": event["event_time"] + timedelta(minutes=duration),
            "bike_id": random.randint(10000, 11999),
            "bike_type": random.choices([1, 2], weights=[80, 20])[0],
            "region": event["h3_area_name"],
            "h3_start_area_name": event["h3_area_name"],
            "h3_end_area_name": event.get("h3_area_name"),
            "user_id": event["user_id"],
            "distance": distance_m,
            "duration": duration * 60,
            "used_min": duration,
            "fee": fee,
            "is_holiday": event["date"].strftime("%Y-%m-%d") in HOLIDAYS,
            "weekday": event["date"].weekday(),
            "center_id": district_info["center_id"],
        })
    return pd.DataFrame(rows)


def generate_maintenance(bikes_df, dates):
    """Generate maintenance task records."""
    rows = []
    task_id = 0
    for date in dates:
        n_tasks = max(0, int(np.random.poisson(15)))
        for _ in range(n_tasks):
            task_id += 1
            bike = bikes_df.sample(1).iloc[0]
            task_type = random.choices([0, 1, 2], weights=[40, 35, 25])[0]
            status = random.choices([0, 1, 2, 3], weights=[5, 5, 10, 80])[0]
            created = datetime.combine(date, datetime.min.time()).replace(
                hour=random.randint(6, 21), minute=random.randint(0, 59))
            completed = created + timedelta(minutes=random.randint(20, 180)) if status == 3 else None

            rows.append({
                "id": task_id,
                "created_time": created,
                "completed_time": completed,
                "created_date": date,
                "completed_date": date if completed else None,
                "type": task_type,  # 0=REBALANCE, 1=BATTERY, 2=BROKEN
                "status": status,   # 0=CALL, 1=CANCEL, 2=PROGRESS, 3=COMPLETE
                "vehicle_id": bike["bike_id"],
                "bike_type": bike["type"],
                "center_id": bike["center_id"],
                "region": bike["region"],
            })
    return pd.DataFrame(rows)


def generate_weather(dates):
    """Generate weather data."""
    rows = []
    for date in dates:
        month = date.month
        # Seasonal temperature patterns
        base_temp = {1: -2, 2: 1, 3: 7, 4: 13, 5: 19, 6: 24,
                     7: 27, 8: 27, 9: 22, 10: 15, 11: 7, 12: 0}
        temp_base = base_temp.get(month, 15)
        temp_low = temp_base + np.random.normal(-3, 2)
        temp_high = temp_base + np.random.normal(5, 2)
        snow_depth = max(0, np.random.normal(-5, 3)) if month in (12, 1, 2) else 0

        rows.append({
            "date": date,
            "temp_low": round(temp_low, 1),
            "temp_high": round(temp_high, 1),
            "precipitation": max(0, round(np.random.exponential(2), 1)) if random.random() < 0.25 else 0,
            "snow_depth": round(snow_depth, 1),
            "wind_speed": round(max(0, np.random.normal(3, 2)), 1),
        })
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser(description="Generate sample bike-share data")
    parser.add_argument("--output", default="./sample_data", help="Output directory")
    parser.add_argument("--days", type=int, default=90, help="Number of days to generate")
    parser.add_argument("--bikes", type=int, default=500, help="Number of bikes")
    parser.add_argument("--snapshot-hours", type=int, nargs="+", default=[7, 12, 18, 21],
                        help="Hours for bike snapshots (full 24h is very large)")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=args.days)
    dates = pd.date_range(start_date, end_date).date.tolist()

    print(f"Generating sample data: {len(dates)} days, {args.bikes} bikes")
    print(f"Output: {args.output}/")

    # 1. Reference data
    print("\n[1/7] Centers & regions...")
    pd.DataFrame(CENTERS).to_csv(f"{args.output}/centers.csv", index=False)
    districts_df = generate_districts()
    districts_df.to_csv(f"{args.output}/districts.csv", index=False)

    # 2. Bikes
    print("[2/7] Bike fleet...")
    bikes_df = generate_bikes(args.bikes)
    bikes_df.to_csv(f"{args.output}/bikes.csv", index=False)

    # 3. Bike snapshots (subset of hours to keep size manageable)
    print(f"[3/7] Bike snapshots (hours: {args.snapshot_hours})...")
    snapshot_df = generate_bike_snapshot(bikes_df, dates, hours=args.snapshot_hours)
    snapshot_df.to_csv(f"{args.output}/bike_snapshot.csv", index=False)
    print(f"       {len(snapshot_df):,} rows")

    # 4. App accessibility
    print("[4/7] App accessibility events...")
    access_df = generate_app_accessibility(districts_df, dates)
    access_df.to_csv(f"{args.output}/app_accessibility.csv", index=False)
    print(f"       {len(access_df):,} rows")

    # 5. Rides
    print("[5/7] Rides...")
    rides_df = generate_rides(access_df, dates)
    rides_df.to_csv(f"{args.output}/rides.csv", index=False)
    print(f"       {len(rides_df):,} rows")

    # 6. Maintenance
    print("[6/7] Maintenance tasks...")
    maint_df = generate_maintenance(bikes_df, dates)
    maint_df.to_csv(f"{args.output}/maintenance.csv", index=False)
    print(f"       {len(maint_df):,} rows")

    # 7. Weather
    print("[7/7] Weather data...")
    weather_df = generate_weather(dates)
    weather_df.to_csv(f"{args.output}/weather.csv", index=False)

    # Summary
    print(f"\nDone! Sample data generated in {args.output}/")
    print(f"  centers.csv       - {len(CENTERS)} service centers")
    print(f"  districts.csv     - {len(districts_df)} districts")
    print(f"  bikes.csv         - {len(bikes_df)} bikes")
    print(f"  bike_snapshot.csv - {len(snapshot_df):,} hourly snapshots")
    print(f"  app_accessibility.csv - {len(access_df):,} app open events")
    print(f"  rides.csv         - {len(rides_df):,} rides")
    print(f"  maintenance.csv   - {len(maint_df):,} maintenance tasks")
    print(f"  weather.csv       - {len(weather_df)} days of weather")


if __name__ == "__main__":
    main()
