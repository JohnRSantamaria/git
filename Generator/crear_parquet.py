import concurrent.futures
from decimal import Decimal
import polars as pl
import os
import uuid
import random
from datetime import datetime, timedelta

from wraps.medir_tiempo import medir_tiempo



class CreadorParquet:
    def __init__(
        self,
        df_base: pl.DataFrame,
        posibles_valores: dict = None,
        proceso_id: str = None,
        columna_incremental: str = None,
        temp_dir: str = "fuentes/temp",
        output_path: str = "fuentes/informes/",
        incrementan: dict = None,
    ) -> None:
        self.df_base = df_base
        self.posibles_valores = posibles_valores or {}
        self.proceso_id = proceso_id or str(uuid.uuid4())
        self.columna_incremental = columna_incremental
        self.temp_dir = temp_dir
        self.output_path = f"{output_path}/{self.proceso_id}.parquet"
        self.incrementan = incrementan or {}

        os.makedirs(self.temp_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)

    def _generar_dato_aleatorio(self, columna: str, dtype: pl.DataType):
        if columna in self.posibles_valores:
            return random.choice(self.posibles_valores[columna])

        if dtype == pl.Utf8:
            return "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=5))
        elif dtype == pl.Int32 or dtype == pl.Int64:
            return random.randint(0, 1000000)
        elif dtype == pl.Float32 or dtype == pl.Float64:
            return random.uniform(0, 1000000)
        elif dtype == pl.Date:
            start_date = datetime(2000, 1, 1)
            return start_date + timedelta(days=random.randint(0, 365 * 30))
        elif dtype == pl.Datetime:
            start_datetime = datetime(2000, 1, 1)
            return start_datetime + timedelta(
                seconds=random.randint(0, 60 * 60 * 24 * 365 * 30)
            )
        elif dtype == pl.Decimal(30, 2):
            return Decimal(random.randint(0, 10**30)) / Decimal(100)
        elif dtype == pl.Decimal(31, 0):
            return str(Decimal(random.randint(0, 10**31 - 1)))
        else:
            return None

    @medir_tiempo
    def generar_datos(self, n_rows: int, batch_size: int = 1000000) -> None:
        temp_files = []
        incrementan = self.incrementan

        for batch_start in range(0, n_rows, batch_size):
            batch_end = min(batch_start + batch_size, n_rows)
            data = {}
            for columna in self.df_base.columns:
                dtype = self.df_base.schema[columna]
                if columna == self.columna_incremental:
                    data[columna] = list(range(batch_start + 1, batch_end + 1))
                elif columna in incrementan:
                    data[columna] = [
                        incrementan[columna] + i for i in range(batch_start, batch_end)
                    ]

                else:
                    data[columna] = [
                        self._generar_dato_aleatorio(columna, dtype)
                        for _ in range(batch_end - batch_start)
                    ]

            batch_df = pl.DataFrame(data)
            temp_file = os.path.join(
                self.temp_dir, f"temp_batch_{batch_start}_{batch_end}.parquet"
            )
            batch_df.write_parquet(temp_file, compression="zstd")
            temp_files.append(temp_file)

        combined_df = pl.concat([pl.scan_parquet(f) for f in temp_files]).collect()
        combined_df.write_parquet(self.output_path, compression="zstd")

        total_rows = combined_df.height
        print(f"Total de filas creadas: {total_rows}")
        # creado en
        print(f"Creado en: {self.output_path}")

        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(os.remove, temp_files)

