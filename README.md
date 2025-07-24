# ETL Postcode Enrichment System

Sistema ETL para enriquecer coordenadas geográficas (lat/lon) con códigos postales del Reino Unido utilizando la API de Postcodes.io.

## Arquitectura de la Solución

### Componentes Principales
### Flujo de Datos

1. **Extracción**: Lectura del archivo CSV con coordenadas geográficas
2. **Transformación**: Limpieza, validación y preparación de datos
3. **Enriquecimiento**: Consulta paralela a la API de Postcodes.io
4. **Carga**: Almacenamiento optimizado en base de datos SQLite
5. **Reporte**: Generación de estadísticas y archivos de salida

## Características Principales

- **Procesamiento en paralelo**: Hasta 17 hilos concurrentes para consultas API
- **Manejo robusto de errores**: Logging detallado y registro de errores de API
- **Optimización de base de datos**: Índices automáticos y consultas eficientes
- **Limitación inteligente**: Procesamiento máximo de 20,000 coordenadas por ejecución
- **Reportes automáticos**: Estadísticas de calidad y exportación de datos

## Prerrequisitos

```bash
pip install pandas sqlalchemy requests
```

### Dependencias del Sistema

- Python 3.7+
- SQLite (incluido en Python)
- Conexión a internet (para API de Postcodes.io)

## Estructura del Proyecto

```
proyecto-etl/
├── main.py                 # Script principal ETL
├── postcodesgeo.csv        # Archivo fuente (requerido)
├── db_postcodes.db         # Base de datos SQLite (generada)
├── enriched_data.csv       # Datos enriquecidos (generado)
├── report_summary.txt      # Reporte de estadísticas (generado)
├── api_errors.log          # Log de errores API (generado)
└── README.md              # Esta documentación
```

## Instalación y Configuración

1. **Clona o descarga el proyecto**
```bash
git clone <repository-url>
cd etl-postcode-project
```

2. **Instala las dependencias**
```bash
pip install -r requirements.txt
```

3. **Prepara el archivo fuente**
   - Coloca tu archivo `postcodesgeo.csv` en el directorio raíz
   - Asegúrate de que contenga las columnas `lat` y `lon`

## Ejecución

### Ejecución Básica
```bash
python main.py
```

### Monitoreo en Tiempo Real
```bash
python main.py | tee etl_execution.log
```

## Archivos de Salida

### 1. Base de Datos SQLite (`db_postcodes.db`)
```sql
-- Tabla principal con índices optimizados
CREATE TABLE locations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lat REAL NOT NULL,
    lon REAL NOT NULL,
    nearest_postcode TEXT,
    UNIQUE(lat, lon)
);
```

### 2. Datos Enriquecidos (`enriched_data.csv`)
Archivo CSV con todas las coordenadas procesadas y sus códigos postales correspondientes.

### 3. Reporte de Estadísticas (`report_summary.txt`)
- Total de coordenadas procesadas
- Porcentaje de cobertura exitosa
- Top 10 códigos postales más comunes
- Métricas de calidad de datos

### 4. Log de Errores (`api_errors.log`)
Registro detallado de errores de API para análisis posterior:
```
lat,lon,mensaje_error
51.5074,-0.1278,Timeout al contactar API
```
### Personalización de Parámetros

Para modificar el comportamiento del sistema, edita las siguientes variables en `main.py`:

- `MAX_COORDS`: Ajusta según tu capacidad de procesamiento
- `max_workers`: Reduce si experimentas problemas de conectividad
- `timeout`: Aumenta para conexiones lentas

## Monitoreo y Logging

### Niveles de Log
- **INFO**: Progreso general y estadísticas
- **ERROR**: Problemas críticos y errores de archivo
- **DEBUG**: Detalles de API y procesamiento (configurable)

### Ejemplo de Salida
```
2024-01-15 10:30:15 - INFO - Archivo cargado correctamente con 25000 registros
2024-01-15 10:30:16 - INFO - Transformación completada. Registros válidos: 24850
2024-01-15 10:30:17 - INFO - → 20/20000 coordenadas procesadas
2024-01-15 10:35:42 - INFO - → 20000/20000 coordenadas procesadas
2024-01-15 10:35:43 - INFO - Enriquecimiento multihilo completado
```

## Troubleshooting

### Problemas Comunes

1. **Error "Archivo no encontrado"**
   ```bash
   # Solución: Verificar que postcodesgeo.csv existe
   ls -la postcodesgeo.csv
   ```

2. **Timeouts frecuentes de API**
   ```python
   # Reducir workers o aumentar timeout
   max_workers = 10  # Reducir de 17
   timeout = 10      # Aumentar de 5
   ```
### Optimizaciones Implementadas
- Procesamiento paralelo con ThreadPoolExecutor
- Índices de base de datos para consultas rápidas
- Manejo eficiente de memoria con iteradores
- Reutilización de conexiones HTTP

## Consideraciones de Seguridad

- Las coordenadas se envían a la API externa de Postcodes.io
- No se almacenan credenciales (API pública)

## Uso

Para contribuir al proyecto:

1. Fork el repositorio
2. Crea una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abre un Pull Request
