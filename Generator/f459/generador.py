import polars as pl

from Generator.crear_parquet import CreadorParquet


fuente459 = pl.DataFrame(
    {
        "UC": pl.Series([], dtype=pl.Utf8),
        "SUBCUENTA": pl.Series([], dtype=pl.Utf8),
        "Tipo_cap": pl.Series([], dtype=pl.Utf8),
        "Numero_ID": pl.Series([], dtype=pl.Int32),
        "Numero_Producto": pl.Series([], dtype=pl.Int32),
        "Valor": pl.Series([], dtype=pl.Decimal(11, 2)),
        "FECHA_CORTE": pl.Series([], dtype=pl.Date),
        "IdDatoFuente": pl.Series([], dtype=pl.Int32),
    }
)


subcuenta1 = ["001", "002", "999"]
subcuenta = [str(i).zfill(3) for i in range(5, 90, 5)]
subcuenta.extend(subcuenta1)

posibles_valores = {
    "UC": [str(i) for i in range(1, 6)],
    "SUBCUENTA": subcuenta,
    "Tipo_cap": [str(i) for i in range(1, 7)],
    "FECHA_CORTE": ["2023-07-31"],
}

creador = CreadorParquet(
    df_base=fuente459,
    posibles_valores=posibles_valores,
    proceso_id="20C27462-C3BE-47B1-A3DE-FA5C5464CE0B",
    columna_incremental="IdDatoFuente",
    output_path="fuentes/informes/f459.parquet",
    incrementan={"Numero_ID": 3332986, "Numero_Producto": 3332987},
)


creador.generar_datos(n_rows=10000000)
