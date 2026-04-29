import pandas as pd
import os

# configuración de rutas en mi pcxd
INPUT_PATH = "/Users/benjamin/Universidad/Sistemas_Distribuidos/Tarea1/data/Raw/open_buildings_v3_points_ne_110m_CHL.csv.gz"
OUTPUT_DIR = "/Users/benjamin/Universidad/Sistemas_Distribuidos/Tarea1/data/processed"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# coordenadas que pedia a tarae
ZONES = {
    "Z1": {"name": "Providencia",     "lat_min": -33.445, "lat_max": -33.420, "lon_min": -70.640, "lon_max": -70.600},
    "Z2": {"name": "Las Condes",      "lat_min": -33.420, "lat_max": -33.390, "lon_min": -70.600, "lon_max": -70.550},
    "Z3": {"name": "Maipu",           "lat_min": -33.530, "lat_max": -33.490, "lon_min": -70.790, "lon_max": -70.740},
    "Z4": {"name": "Santiago Centro", "lat_min": -33.460, "lat_max": -33.430, "lon_min": -70.670, "lon_max": -70.630},
    "Z5": {"name": "Pudahuel",        "lat_min": -33.470, "lat_max": -33.430, "lon_min": -70.810, "lon_max": -70.760},
}

def filtrar():
    print(f"Leyendo dataset comprimido: {INPUT_PATH}")
    try:
        
        # usamos las columnas que nos piden: latitude, longitude, area_in_meters, confidence 
        df = pd.read_csv(INPUT_PATH, compression='gzip', engine='c')
        
        # limpiar posibles espacios en los nombres de las columnas
        df.columns = [c.strip() for c in df.columns]
        
        print(f"Total de registros cargados de Chile: {len(df):,}")

        for zid, info in ZONES.items():
            
            mask = (
                (df["latitude"] >= info["lat_min"]) & (df["latitude"] <= info["lat_max"]) &
                (df["longitude"] >= info["lon_min"]) & (df["longitude"] <= info["lon_max"])
            )
            df_zona = df[mask].copy()
            df_zona["zone_id"] = zid
            
            # se guarda cada zona en un csv individual en 'data/processed'
            file_name = f"{info['name'].replace(' ', '_')}.csv"
            output_path = os.path.join(OUTPUT_DIR, file_name)
            df_zona.to_csv(output_path, index=False)
            print(f" -> {info['name']} ({zid}): {len(df_zona):,} edificios guardados en {file_name}")

        print("\nFiltrado completado con éxito.")
        
    except Exception as e:
        print(f"Error al procesar el archivo: {e}")

if __name__ == "__main__":
    filtrar()