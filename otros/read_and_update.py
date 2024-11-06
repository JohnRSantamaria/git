import polars as pl


def edicion_parquet(parquet_file):
    df = pl.read_parquet(parquet_file)

    print("Columnas disponibles:", df.columns)

    if (
        "COLUMNA" in df.columns
        and "SUBCUENTA" in df.columns
        and "VALORES" in df.columns
    ):
        # Modifica "COLUMNA" cuando sea igual a 51
        df = df.with_columns(
            pl.when(pl.col("COLUMNA") == 51)
            .then(52)
            .otherwise(pl.col("COLUMNA"))
            .alias("COLUMNA")
        )

        # Primera condición para "VALORES" basada en "COLUMNA"
        df = df.with_columns(
            pl.when(pl.col("COLUMNA") == 52)
            .then(pl.lit("1352"))
            .otherwise(pl.col("VALORES"))
            .alias("VALORES")
        )

        # Segunda condición para "VALORES" basada en "SUBCUENTA"
        df = df.with_columns(
            pl.when(
                (pl.col("COLUMNA") == 52)
                & (pl.col("SUBCUENTA").is_in(["6", "7", "8", "9"]))
            )
            .then(pl.lit("2230"))
            .otherwise(pl.col("VALORES"))
            .alias("VALORES")
        )

        df.write_parquet("7fb15c6b-192b-425a-b9d3-b173fa8ac82f.parquet")
    else:
        print("Una o más columnas no están en el archivo Parquet original.")


edicion_parquet("7fb15c6b-192b-425a-b9d3-b173fa8ac82f(original).parquet")
