from loguru import logger # type: ignore
from functools import wraps
import time

def log_decorator(func):
    """
    Decorador para adicionar logging antes e após a execução de uma função.

    Registra uma mensagem informativa no início e ao final da execução da função,
    indicando que a função começou e terminou. Em caso de erro, registra a mensagem 
    de erro antes de relançar a exceção.

    Parameters:
        func (callable): A função que será decorada.

    Returns:
        callable: A função decorada que inclui logs antes e depois de sua execução.

    Raises:
        Exception: Relança quaisquer exceções que ocorram durante a execução da função decorada.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            logger.info(f'Executando a função {func.__name__}.')
            result = func(*args, **kwargs)
            logger.info(f'Execução da função {func.__name__} finalizada.')
            return result
        except Exception as e:
            logger.error(f'Erro: {e}')
            raise
    return wrapper

def time_decorator(func):
    """
    Decorador para medir o tempo de execução de uma função.

    Calcula o tempo necessário para executar a função e registra uma mensagem 
    informativa com a duração da execução. Em caso de erro, registra a mensagem 
    de erro antes de relançar a exceção.

    Parameters:
        func (callable): A função que será decorada.

    Returns:
        callable: A função decorada que registra o tempo de execução.

    Raises:
        Exception: Relança quaisquer exceções que ocorram durante a execução da função decorada.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            total_time = end_time - start_time
            logger.info(f'A função {func.__name__} levou {total_time:.2f} segundos para finalizar.')
            return result
        except Exception as e:
            logger.error(f'Erro: {e}')
            raise
    return wrapper
