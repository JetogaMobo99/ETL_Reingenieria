from prefect import flow, task
from prefect.logging import get_run_logger

@task
def say_hello():
    """Tarea que dice hola"""
    print("Hello, World!!!!!!!!!!!!!!!!!!!!!!!")
    logger = get_run_logger()
    logger.info("Hello, World! from Prefect Task!!!!!!!!!!!!!!!!!!!!!!!!!!")
    return "Task completed successfully!!!!!!!!!!!!!!!!!!!!!!!!"

@flow(log_prints=True)
def hello_flow():
    """Flow principal"""
    result = say_hello()
    print(f"Flow result: {result}")
    return result

# Para pruebas locales (opcional)
if __name__ == "__main__":
    hello_flow()