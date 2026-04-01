import json
import os

input_file = '../GlobalWeatherRepository[1](1).csv'
output_file = 'data_v2.js'

cities_dict = {}

with open(input_file, mode='r', encoding='utf-8', errors='replace') as f:
    headers_line = f.readline().strip()
    headers = [h.strip('"') for h in headers_line.split(',')]
    
    def get_idx(col):
        try: return headers.index(col)
        except: return -1

    idx_city = get_idx('location_name')
    if idx_city == -1: idx_city = get_idx('country')
    
    idx_country = get_idx('country')
    idx_lat = get_idx('latitude')
    idx_lon = get_idx('longitude')
    idx_tz = get_idx('timezone')
    idx_last_updated = get_idx('last_updated')
    idx_temp = get_idx('temperature_celsius')
    idx_temp_f = get_idx('temperature_fahrenheit')
    idx_feels = get_idx('feels_like_celsius')
    idx_feels_f = get_idx('feels_like_fahrenheit')
    idx_humidity = get_idx('humidity')
    idx_wind_kph = get_idx('wind_kph')
    idx_wind_mph = get_idx('wind_mph')
    idx_wind_dir = get_idx('wind_direction')
    idx_wind_deg = get_idx('wind_degree')
    idx_gust_kph = get_idx('gust_kph')
    idx_gust_mph = get_idx('gust_mph')
    idx_precip = get_idx('precip_mm')
    idx_precip_in = get_idx('precip_in')
    idx_visibility = get_idx('visibility_km')
    idx_visibility_mi = get_idx('visibility_miles')
    idx_uv = get_idx('uv_index')
    idx_cloud = get_idx('cloud')
    idx_pressure = get_idx('pressure_mb')
    idx_pressure_in = get_idx('pressure_in')
    idx_condition = get_idx('condition_text')
    idx_sunrise = get_idx('sunrise')
    idx_sunset = get_idx('sunset')
    idx_moonrise = get_idx('moonrise')
    idx_moonset = get_idx('moonset')
    idx_moon_phase = get_idx('moon_phase')
    idx_moon_illum = get_idx('moon_illumination')
    idx_pm25 = get_idx('air_quality_PM2.5')
    idx_pm10 = get_idx('air_quality_PM10')
    idx_no2 = get_idx('air_quality_Nitrogen_dioxide')
    idx_o3 = get_idx('air_quality_Ozone')
    idx_co = get_idx('air_quality_Carbon_Monoxide')
    idx_so2 = get_idx('air_quality_Sulphur_dioxide')
    idx_aqi = get_idx('air_quality_us-epa-index')
    idx_gb_defra = get_idx('air_quality_gb-defra-index')

    for line in f:
        row = [x.strip().strip('"') for x in line.split(',')]
        
        if len(row) <= idx_city or not row[idx_city]:
            continue
            
        def safe_float(idx, default=0.0):
            if idx == -1 or idx >= len(row): return default
            try: return float(row[idx])
            except: return default

        def safe_str(idx, default=""):
            if idx == -1 or idx >= len(row): return default
            return row[idx]

        city_name = safe_str(idx_city, "Unknown")
        if city_name in cities_dict:
            continue

        try:
            city_data = {
                'city': city_name,
                'country': safe_str(idx_country, "Unknown"),
                'lat': safe_float(idx_lat, 0),
                'lon': safe_float(idx_lon, 0),
                'tz': safe_str(idx_tz, ''),
                'last_updated': safe_str(idx_last_updated, ''),
                'temp': safe_float(idx_temp, 0),
                'temp_f': safe_float(idx_temp_f, 32),
                'feels': safe_float(idx_feels, 0),
                'feels_f': safe_float(idx_feels_f, 32),
                'humidity': safe_float(idx_humidity, 0),
                'wind_kph': safe_float(idx_wind_kph, 0),
                'wind_mph': safe_float(idx_wind_mph, 0),
                'wind_dir': safe_str(idx_wind_dir, 'N'),
                'wind_deg': safe_float(idx_wind_deg, 0),
                'gust_kph': safe_float(idx_gust_kph, 0),
                'gust_mph': safe_float(idx_gust_mph, 0),
                'precip': safe_float(idx_precip, 0),
                'precip_in': safe_float(idx_precip_in, 0),
                'visibility': safe_float(idx_visibility, 10),
                'visibility_mi': safe_float(idx_visibility_mi, 6),
                'uv': safe_float(idx_uv, 0),
                'cloud': safe_float(idx_cloud, 0),
                'pressure': safe_float(idx_pressure, 1013),
                'pressure_in': safe_float(idx_pressure_in, 29.92),
                'condition': safe_str(idx_condition, 'Clear'),
                'sunrise': safe_str(idx_sunrise, '06:00 AM'),
                'sunset': safe_str(idx_sunset, '06:00 PM'),
                'moonrise': safe_str(idx_moonrise, ''),
                'moonset': safe_str(idx_moonset, ''),
                'moon_phase': safe_str(idx_moon_phase, 'Full Moon'),
                'moon_illum': safe_str(idx_moon_illum, '50'),
                'pm25': safe_float(idx_pm25, 0),
                'pm10': safe_float(idx_pm10, 0),
                'no2': safe_float(idx_no2, 0),
                'o3': safe_float(idx_o3, 0),
                'co': safe_float(idx_co, 0),
                'so2': safe_float(idx_so2, 0),
                'aqi': int(safe_float(idx_aqi, 1)),
                'gb_defra': int(safe_float(idx_gb_defra, 1))
            }
            cities_dict[city_name] = city_data
        except Exception as e:
            continue

with open(output_file, 'w', encoding='utf-8') as out:
    json_str = json.dumps(list(cities_dict.values()))
    out.write(f"window.CITIES = {json_str};\n")

print(f"Successfully processed {len(cities_dict)} unique entries to {output_file}")
