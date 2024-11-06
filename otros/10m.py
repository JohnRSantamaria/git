import random
import polars as pl

# Valores posibles
tipos_cap = ["1", "2", "3", "4", "5", "6"]
ucs = ["1", "2", "3", "4", "5"]
subcuentas = [
    "001",
    "002",
    "005",
    "010",
    "015",
    "020",
    "025",
    "030",
    "035",
    "040",
    "045",
    "050",
    "055",
    "060",
    "065",
    "070",
    "075",
    "080",
    "085",
    "999",
]

# Configuraciones fijas
fecha_corte = "31/07/2023"
total_registros = 10_000_000  # Número total de registros a generar
chunk_size = 1_000_000  # Tamaño del chunk para la paginación


# Función para generar valores numéricos con longitud fija
def generar_valor(longitud=31):
    return "".join([str(random.randint(0, 9)) for _ in range(longitud)])


# Lista para almacenar datos
datos = []

# Generar datos en chunks
for i in range(0, total_registros, chunk_size):
    chunk = []
    id_dato_fuente = i + 1  # ID de datos fuente comienza desde 1
    numero_id_inicial = 3332986 + i
    numero_producto_inicial = 3332987 + i

    for _ in range(chunk_size):
        if len(datos) >= total_registros:
            break  # Salir si se alcanza el total de registros

        tipo_cap = random.choice(tipos_cap)
        uc = random.choice(ucs)
        subcuenta = random.choice(subcuentas)

        # Crear un nuevo registro
        nuevo_registro = {
            "UC": str(uc),
            "SUBCUENTA": str(subcuenta),
            "Tipo_cap": str(tipo_cap),
            "Numero_ID": numero_id_inicial,
            "Numero_Producto": numero_producto_inicial,
            "Valor": generar_valor(),
            "FECHA_CORTE": fecha_corte,
            "IdDatoFuente": id_dato_fuente,
        }

        # Agregar el registro al chunk
        chunk.append(nuevo_registro)

        # Incrementar los valores
        id_dato_fuente += 1
        numero_id_inicial += 1
        numero_producto_inicial += 1

    # Agregar el chunk a la lista principal de datos
    datos.extend(chunk)

# Crear un DataFrame de Polars a partir de los datos generados
df = pl.DataFrame(datos)

# Convertir las columnas 'UC', 'SUBCUENTA' y 'Tipo_cap' a tipo string explícitamente
df = df.with_columns(
    [
        pl.col("UC").cast(pl.Utf8),
        pl.col("SUBCUENTA").cast(pl.Utf8),
        pl.col("Tipo_cap").cast(pl.Utf8),
        pl.col("FECHA_CORTE").cast(pl.Utf8),  # Mantener como string por ahora
    ]
)

# Convertir la columna 'FECHA_CORTE' a tipo fecha
df = df.with_columns(pl.col("FECHA_CORTE").str.strptime(pl.Date, "%d/%m/%Y"))

# Guardar el DataFrame en un solo archivo Parquet
data_uuid = "datos_generados.parquet"
df.write_parquet(data_uuid)

print(f"Datos guardados en '{data_uuid}'.")
