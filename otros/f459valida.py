import csv
import random
import pandas as pd

# Configuraciones fijas
fecha_corte = "31/07/2023"
id_dato_fuente_inicial = 1
codigos_cuif = [
    "2105",
    "2106",
    "2108",
    "210815",
    "2109",
    "2107",
    "2110",
    "213005",
    "224505",
    "213006",
    "224506",
    "213007",
    "224507",
    "213008",
    "224508",
    "213009",
    "224509",
    "213010",
    "224510",
    "213011",
    "224511",
    "213012",
    "224512",
    "213013",
    "224513",
    "2120",
]


# Función para generar valores numéricos decimales con formato aleatorio
def generar_descripcion():
    return str(random.uniform(0, 1))  # Genera un número decimal aleatorio entre 0 y 1


# Función para generar un código CUIF aleatorio
def generar_codigo_cuif():
    return random.choice(codigos_cuif)  # Selecciona un valor de la lista codigos_cuif


# Función para generar un código de moneda aleatorio
def generar_moneda0():
    return str(random.randint(1, 99))  # Genera un número de moneda entre 1 y 99


# Función para generar los datos
def generar_datos():
    id_dato_fuente = id_dato_fuente_inicial
    datos = []

    for _ in range(100):  # Generar 100 registros (puedes cambiar este número)
        nuevo_registro = {
            "CODIGO_CUIF": generar_codigo_cuif(),  # Código aleatorio de CUIF
            "DESCRIPCION": generar_descripcion(),  # Valor decimal aleatorio
            "MONEDA0": generar_moneda0(),  # Código aleatorio de moneda
            "FECHA_CORTE": fecha_corte,  # Fecha constante
            "IdDatoFuente": id_dato_fuente,  # Incrementa con cada registro
        }

        datos.append(nuevo_registro)
        id_dato_fuente += 1  # Incrementar IdDatoFuente

    return datos


# Generar los datos
datos_generados = generar_datos()

# Escribir los datos en un archivo CSV
with open(
    "datos_generados_cuif.csv", mode="w", newline="", encoding="utf-8"
) as archivo_csv:
    fieldnames = [
        "CODIGO_CUIF",
        "DESCRIPCION",
        "MONEDA0",
        "FECHA_CORTE",
        "IdDatoFuente",
    ]
    writer = csv.DictWriter(archivo_csv, fieldnames=fieldnames)

    # Escribir la cabecera
    writer.writeheader()

    # Escribir los registros
    for dato in datos_generados:
        writer.writerow(dato)

print("Archivo CSV generado exitosamente.")

# UUID para el archivo Parquet
data_uuid = "1BE37F63-1BD1-40AA-BDE0-E5AF632CCBE6"

# Cargar el CSV generado previamente en un DataFrame
df = pd.read_csv("datos_generados_cuif.csv")

# Convertir las columnas 'CODIGO_CUIF', 'DESCRIPCION' y 'MONEDA0' a tipo string explícitamente
df["CODIGO_CUIF"] = df["CODIGO_CUIF"].astype(str)
df["DESCRIPCION"] = df["DESCRIPCION"].astype(str)
df["MONEDA0"] = df["MONEDA0"].astype(str)

# Convertir la columna 'FECHA_CORTE' a tipo fecha (asegurando el formato correcto)
df["FECHA_CORTE"] = pd.to_datetime(df["FECHA_CORTE"], format="%d/%m/%Y")

# Si prefieres que se guarde como string en el formato "DD-MM-YYYY", convierte nuevamente a string:
df["FECHA_CORTE"] = df["FECHA_CORTE"].dt.strftime("%d/%m/%Y")

# Guardar el DataFrame en un archivo Parquet
df.to_parquet(f"{data_uuid}.parquet", engine="pyarrow")

print(f"Datos convertidos a Parquet y guardados en '{data_uuid}.parquet'.")
