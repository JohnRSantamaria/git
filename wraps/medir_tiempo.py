import time
from functools import wraps


def medir_tiempo(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()

        elapsed_time = end_time - start_time
        hours = int(elapsed_time // 3600)
        minutes = int((elapsed_time % 3600) // 60)
        seconds = int(elapsed_time % 60)

        print(
            f"Tiempo de ejecución de '{func.__name__}': {hours} horas, {minutes} minutos, {seconds} segundos\n"
        )
        return result

    return wrapper
