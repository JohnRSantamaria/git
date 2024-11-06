import polars as pl
import json

def generar_datos_unicos_combinados_parquet(parquet_path, nombre_archivo):
    """
    Lee un archivo Parquet, extrae los valores únicos combinados de todas las columnas
    y los guarda en un archivo JSON.

    :param parquet_path: Ruta del archivo Parquet.
    :param nombre_archivo: Nombre del archivo JSON que se generará.
    """
    try:
        # Leer el archivo Parquet
        df = pl.read_parquet(parquet_path)

        # Crear un conjunto para almacenar los valores únicos de todas las columnas
        valores_unicos = set()

        # Iterar sobre todas las columnas y agregar los valores únicos al conjunto
        for column in df.columns:
            valores_unicos.update(df[column].unique().to_list())  # Agregar valores únicos

        # Convertir el conjunto a lista para guardar en JSON
        valores_unicos_lista = list(valores_unicos)

        # Guardar la lista de valores únicos combinados como un archivo JSON
        with open(nombre_archivo, "w", encoding="utf-8") as json_file:
            json.dump(valores_unicos_lista, json_file, ensure_ascii=False, indent=4)

        print(f"Archivo JSON '{nombre_archivo}' generado exitosamente con valores únicos combinados.")

    except Exception as e:
        print(f"Error al generar el archivo JSON '{nombre_archivo}': {str(e)}")
        raise e

# Uso de la función
parquet_path = r"\B15DB981-E2EF-43F2-87F4-EBB7F82F05EF.parquet"
nombre_archivo_json = "valores_unicos_combinados.json"

generar_datos_unicos_combinados_parquet(parquet_path, nombre_archivo_json)
