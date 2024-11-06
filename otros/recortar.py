import polars as pl
import numpy as np

# Cargar los primeros 1000 registros del archivo Parquet
df = pl.scan_parquet("veintemillones.parquet").limit(1000).collect()

# Generar valores aleatorios entre 1 y 50 para la columna 'PLAZO_CAP'
# Convertirlos a string (Utf8)
df = df.with_columns([
    pl.lit(np.random.randint(1, 51, df.height).astype(str)).alias("PLAZO_CAP"),
    pl.lit(1).alias("RENOVACIONES_TIPO_CAP")    
])

# Convertir el DataFrame a CSV
df.write_csv("salida.csv")

# Guardar los resultados en formato Parquet
df.write_parquet("B15DB981-E2EF-43F2-87F4-EBB7F82F05EF.parquet")
