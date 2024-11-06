import polars as pl

from Generator.crear_parquet import CreadorParquet

fuente441 = pl.DataFrame({})

posibles_valores = []

creador = CreadorParquet(
    df_base=fuente441,
    posibles_valores=posibles_valores,
    columna_incremental="IdDatoFuente",
    output_path="fuentes/informes/f441.parquet",
)


creador.generar_datos(n_rows=20000000)
