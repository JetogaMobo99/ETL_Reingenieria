

from prefect import flow, task
from prefect.logging import get_run_logger
from etl_scripts import *
from etl_scripts.f4_etl_cadenas_sell_out import etl_cadenas_sell_out
from prefect.context import get_run_context
from utils import *

@task
def etl_cadenas_sellout():
    """ETL process for cadenas sell out data"""
    logger = get_run_logger()
    logger.info("Starting ETL process for cadenas sell out")
    etl_cadenas_sell_out()
    logger.info("ETL process for cadenas sell out completed successfully.")

@flow(log_prints=True)
def etl_cadenas_sell_out_flow():
    """Main flow for cadenas sell out ETL"""
    etl_cadenas_sellout()

if __name__ == "__main__":
    etl_cadenas_sell_out_flow()