import os
from prefect import flow, task
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())


@flow(name="get_environment_flow", log_prints=True)
def get_environment_flow():
    """Obtiene el entorno actual desde las variables de entorno"""
    env = os.getenv("ENVIRONMENT").lower()
    print(f"Entorno actual: {env}")
    return env


if __name__ == "__main__":
    get_environment_flow()