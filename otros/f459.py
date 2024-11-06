import csv
import random
import pandas as pd

# Valores posibles
tipos_cap = ["1", "2", "3", "4", "5", "6"]
ucs = ["1","2" ,"3", "4", "5"]
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
id_dato_fuente_inicial = 1


# Función para generar valores numéricos con longitud fija
def generar_valor(longitud=31):
    return "".join([str(random.randint(0, 9)) for _ in range(longitud)])


# Función para generar datos
def generar_datos():
    id_dato_fuente = id_dato_fuente_inicial
    numero_id_inicial = 3332986
    numero_producto_inicial = 3332987

    datos = []

    for tipo_cap in tipos_cap:
        for uc in ucs:
            for subcuenta in subcuentas:
                # Crear un nuevo registro con las variaciones necesarias
                nuevo_registro = {
                    "UC": str(uc),  # Asegurarse de que UC sea string
                    "SUBCUENTA": str(
                        subcuenta
                    ),  # Asegurarse de que SUBCUENTA sea string
                    "Tipo_cap": str(tipo_cap),  # Asegurarse de que Tipo_cap sea string
                    "Numero_ID": numero_id_inicial,  # Mantener como entero
                    "Numero_Producto": numero_producto_inicial,  # Mantener como entero
                    "Valor": generar_valor(),  # No usar comillas extra
                    "FECHA_CORTE": fecha_corte,
                    "IdDatoFuente": id_dato_fuente,  # Mantener como entero
                }

                # Agregar registro a la lista de datos
                datos.append(nuevo_registro)

                # Incrementar los valores
                id_dato_fuente += 1
                numero_id_inicial += 1
                numero_producto_inicial += 1

    return datos


# Generar los datos
f459_variado = generar_datos()

# Escribir los datos en un archivo CSV
with open("datos_generados.csv", mode="w", newline="", encoding="utf-8") as archivo_csv:
    fieldnames = [
        "UC",
        "SUBCUENTA",
        "Tipo_cap",
        "Numero_ID",
        "Numero_Producto",
        "Valor",
        "FECHA_CORTE",
        "IdDatoFuente",
    ]
    writer = csv.DictWriter(archivo_csv, fieldnames=fieldnames)

    # Escribir la cabecera
    writer.writeheader()

    # Escribir los registros
    for dato in f459_variado:
        writer.writerow(dato)

print("Archivo CSV generado exitosamente.")

# UUID para el archivo Parquet
data_uuid = "7D7E5D4F-3221-41B9-936C-BF00FD4918F6"

# Cargar el CSV generado previamente
df = pd.read_csv("datos_generados.csv")

# Convertir las columnas 'UC', 'SUBCUENTA' y 'Tipo_cap' a tipo string explícitamente
df["UC"] = df["UC"].astype(str)
df["SUBCUENTA"] = df["SUBCUENTA"].astype(str).str.zfill(3)  # Rellenar con 3 dígitos
df["Tipo_cap"] = df["Tipo_cap"].astype(str)

# Convertir la columna 'FECHA_CORTE' a tipo fecha (asegurando el formato correcto)
df["FECHA_CORTE"] = pd.to_datetime(df["FECHA_CORTE"], format="%d/%m/%Y")

# Si prefieres que se guarde como string en el formato "YYYY-MM-DD", convierte nuevamente a string:
df["FECHA_CORTE"] = df["FECHA_CORTE"].dt.strftime("%d/%m/%Y")

# Guardar el DataFrame en un archivo Parquet
df.to_parquet(f"{data_uuid}.parquet", engine="pyarrow")

print(f"Datos convertidos a Parquet y guardados en '{data_uuid}.parquet'.")
