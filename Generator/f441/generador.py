import polars as pl

from Generator.crear_parquet import CreadorParquet

fuente441 = pl.DataFrame(
    {
        "CONSECUTIVO": pl.Series([], dtype=pl.Int32),
        "TIPO_CLIENTE": pl.Series([], dtype=pl.Utf8),
        "TIPO_CAP": pl.Series([], dtype=pl.Utf8),
        "CLASE_AHORRO": pl.Series([], dtype=pl.Utf8),
        "ORIGEN_CAPTACION": pl.Series([], dtype=pl.Utf8),
        "PLAZO_CAP": pl.Series([], dtype=pl.Utf8),
        "RENOVACIONES_TIPO_CAP": pl.Series([], dtype=pl.Int32),
        "TIPO_OPERACION": pl.Series([], dtype=pl.Utf8),
        "TIPO_MONEDA": pl.Series([], dtype=pl.Utf8),
        "TIPO_ENTIDAD": pl.Series([], dtype=pl.Utf8),
        "VALOR_CAP": pl.Series([], dtype=pl.Decimal(11, 2)),
        "SALDO_AL_CIERRE": pl.Series([], dtype=pl.Decimal(11, 2)),
        "TASA_PROMEDIO": pl.Series([], dtype=pl.Utf8),
        "FECHA_APERTURA": pl.Series([], dtype=pl.Date),
        "FECHA_SALDO_CIERRE": pl.Series([], dtype=pl.Date),
        "IdDatoFuente": pl.Series([], dtype=pl.Int32),
    }
)

posibles_valores = {
    "TIPO_CLIENTE": [str(i) for i in range(1, 4)],
    "CLASE_AHORRO": [str(i) for i in range(0, 8)],
    "ORIGEN_CAPTACION": [str(i) for i in range(0, 3)],
    "PLAZO_CAP": [str(i) for i in range(1, 51)],
    "RENOVACIONES_TIPO_CAP": [1],
    "TIPO_OPERACION": [str(i) for i in range(0, 3)],
    "TIPO_MONEDA": [str(i) for i in range(0, 3)],
    "TIPO_ENTIDAD": [str(i) for i in range(0, 14)],
    "TASA_PROMEDIO": ["999.99", "1000"],
    "FECHA_APERTURA": ["06/01/2022"],
    "FECHA_SALDO_CIERRE": ["13/07/2023"],
}


creador = CreadorParquet(
    df_base=fuente441,
    posibles_valores=posibles_valores,
    columna_incremental="IdDatoFuente",
    output_path="fuentes/informes/f441.parquet",
    incrementan={"CONSECUTIVO": 1},
)


creador.generar_datos(n_rows=20000000)
