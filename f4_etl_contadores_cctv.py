from prefect import flow, task
from prefect.logging import get_run_logger
from etl_scripts import *
from etl_scripts.f4_etl_contadores_cctv import procesar_contadores
from prefect.context import get_run_context
from utils import *

@task
def etl_contadorescctv():
    """ETL process for contadores CCTV  data"""
    logger = get_run_logger()
    logger.info("Starting ETL process for contadores CCTV")
    procesar_contadores()
    logger.info("ETL process for contadores CCTV completed successfully.")

@flow(log_prints=True)
def contadores_cctv_flow():
    """Main flow for contadores CCTV ETL"""
    etl_contadorescctv()

if __name__ == "__main__":
    contadores_cctv_flow(name="contadores_cctv_flow")