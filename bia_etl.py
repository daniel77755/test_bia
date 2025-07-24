import pandas as pd
from sqlalchemy import create_engine
import logging
import os
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlite3


# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Log estructurado para errores de API
error_log_file = 'api_errors.log'
if os.path.exists(error_log_file):
    os.remove(error_log_file)
    
##BASE DE DATOS (SQLite) ===
SQLITE_DB_PATH = 'db_postcodes.db'
TABLE_NAME = 'codes_postals'

# URI para conexión SQLite
DB_URI = f'sqlite:///{SQLITE_DB_PATH}'


# Función: Extraer datos
def extract_data(file_path):
    try:
        df = pd.read_csv(file_path)
        logging.info("Archivo cargado correctamente con %d registros", len(df))
        return df
    except FileNotFoundError:
        logging.error("Archivo no encontrado: %s", file_path)
        raise
    except pd.errors.EmptyDataError:
        logging.error("El archivo está vacío")
        raise
    except Exception as e:
        logging.error("Error al leer el archivo: %s", str(e))
        raise


# Función: Validar y transformar datos
def transform_data(df):
    # Eliminar duplicados
    df = df.drop_duplicates()
    
    # Validar columnas necesarias
    required_cols = ['lat', 'lon']
    for col in required_cols:
        if col not in df.columns:
            logging.info(f"Columna requerida faltante: {col}")
            continue

    # Limpiar datos: eliminar registros con datos faltantes
    df = df.dropna(subset=required_cols)
    
    # Convertir tipos de datos
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
    print(df)
    # Eliminar registros con coordenadas inválidas
    #df = df.dropna(subset=['lati', 'long'])

    logging.info("Transformación completada. Registros válidos: %d", len(df))
    return df

# Petición individual a la API con manejo de errores
def fetch_postcode(lat, lon):
    try:
        response = requests.get(
            f"https://api.postcodes.io/postcodes?lon={lon}&lat={lat}",
            timeout=5
        )
        if response.status_code == 200:
            result = response.json()
            if result['status'] == 200 and result['result']:
                return result['result'][0]['postcode']
            else:
                log_api_error(lat, lon, "Respuesta sin resultados válidos")
        else:
            log_api_error(lat, lon, f"Error HTTP {response.status_code}")
    except requests.exceptions.Timeout:
        log_api_error(lat, lon, "Timeout al contactar API")
    except requests.exceptions.RequestException as e:
        log_api_error(lat, lon, f"Error de red: {str(e)}")
    return None

# Enriquecer usando multihilos
def enrich_with_api(df):
    #MAX_COORDS = 20000
    #coords = list(zip(df['lat'], df['lon']))[:MAX_COORDS] 
    coords = list(zip(df['lat'], df['lon']))
    postcodes = [None] * len(df)

    with ThreadPoolExecutor(max_workers=17) as executor:
        future_to_index = {
            executor.submit(fetch_postcode, lat, lon): i
            for i, (lat, lon) in enumerate(coords)
        }

        for count, future in enumerate(as_completed(future_to_index), 1):
            index = future_to_index[future]
            try:
                result = future.result()
                postcodes[index] = result
            except Exception as e:
                logging.error(f"Error inesperado en hilo: {str(e)}")
            if count % 20 == 0:
                logging.info("→ %d/%d coordenadas procesadas", count, len(coords))

    df['nearest_postcode'] = postcodes
    logging.info("Enriquecimiento multihilo completado")
    return df


# Log de errores API
def log_api_error(lat, lon, message):
    with open(error_log_file, 'a') as f:
        f.write(f"{lat},{lon},{message}\n")


# Función: Cargar en la base de datos
# Crear tabla normalizada e índices
def load_data_optimized(df, db_uri):
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS locations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            nearest_postcode TEXT,
            UNIQUE(lat, lon)
        );
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_locations_postcode ON locations(nearest_postcode);
    """)

    insert_query = """
        INSERT OR IGNORE INTO locations (lat, lon, nearest_postcode)
        VALUES (?, ?, ?);
    """
    rows = list(df[['lat', 'lon', 'nearest_postcode']].itertuples(index=False, name=None))
    cursor.executemany(insert_query, rows)
    conn.commit()
    conn.close()
    logging.info("Datos cargados en tabla 'locations' con índices.")


def generate_report():
    logging.info("Generando reportes y estadísticas...")

    conn = sqlite3.connect(SQLITE_DB_PATH)
    df = pd.read_sql_query("SELECT * FROM locations", conn)

    # 1. Postcodes más comunes
    top_postcodes = df['nearest_postcode'].value_counts().head(10)
    
    # 2. Estadísticas de calidad
    total = len(df)
    with_postcode = df['nearest_postcode'].notna().sum()
    without_postcode = df['nearest_postcode'].isna().sum()
    coverage_pct = round((with_postcode / total) * 100, 2)

    # 3. Exportar CSV con los datos enriquecidos
    df.to_csv("enriched_data.csv", index=False)
    
    # 4. Guardar resumen en TXT
    with open("report_summary.txt", "w") as f:
        f.write(f"Reporte de Datos Enriquecidos\n")
        f.write(f"Total de coordenadas: {total}\n")
        f.write(f"Con código postal: {with_postcode}\n")
        f.write(f"Sin código postal: {without_postcode}\n")
        f.write(f"Porcentaje de cobertura: {coverage_pct}%\n\n")
        f.write("Top 10 códigos postales más comunes:\n")
        f.write(top_postcodes.to_string())
    
    logging.info("Reportes generados: enriched_data.csv y report_summary.txt")
    conn.close()


# Main ETL flow
def run_etl():
    file_path = 'postcodesgeo.csv'
    logging.info("=== INICIANDO PROCESO ETL ===")
    df_raw = extract_data(file_path)
    df_clean = transform_data(df_raw)
    df_enriched = enrich_with_api(df_clean)
    load_data_optimized(df_enriched, DB_URI)
    if os.path.exists(error_log_file):
        logging.info(f"Errores de API registrados en: {error_log_file}")
    logging.info("PROCESO COMPLETADO")
    generate_report()
    
if __name__ == '__main__':
    run_etl()
