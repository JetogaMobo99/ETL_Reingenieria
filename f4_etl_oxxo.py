import sys
import os
# Agregar el directorio raíz al Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from prefect import flow, task
from prefect.logging import get_run_logger
from etl_scripts import *
from etl_scripts.f4_etl_oxxo import etl_sell_out_oxxo, inv_cedis_oxxo, inv_tiendas_oxxo
from prefect.context import get_run_context
from utils import *
import prefect
prefect.settings.log_to_server = True  # ACTIVAR envío de logs



@task
def etl_sell_out_task():
    """Task for OXXO sell out ETL"""
    logger = get_run_logger()
    logger.info("Starting sell out ETL for OXXO")
    etl_sell_out_oxxo()
    logger.info("Sell out ETL for OXXO completed")

@task
def inv_cedis_task():
    """Task for OXXO CEDIS inventory ETL"""
    logger = get_run_logger()
    logger.info("Starting CEDIS inventory ETL for OXXO")
    inv_cedis_oxxo()
    logger.info("CEDIS inventory ETL for OXXO completed")

@task
def inv_tiendas_task():
    """Task for OXXO stores inventory ETL"""
    logger = get_run_logger()
    logger.info("Starting stores inventory ETL for OXXO")
    inv_tiendas_oxxo()
    logger.info("Stores inventory ETL for OXXO completed")


@task
def process_tables():
    """Task to process tables after ETL"""
    logger = get_run_logger()
    logger.info("Processing tables after OXXO ETL")
    process_table(["Sell_out", "inventario_CEDIS_tienda", "inventario_CEDIS"], type='full')
    logger.info("Tables processed successfully.")

@flow(log_prints=True)
def etl_oxxo_flow():
    """ETL process for OXXO data with concurrent execution"""
    logger = get_run_logger()
    logger.info("Starting ETL process for OXXO")
    
    # Run tasks concurrently
    sell_out_future = etl_sell_out_task.submit()
    cedis_future = inv_cedis_task.submit()
    tiendas_future = inv_tiendas_task.submit()
    
    # Wait for all tasks to complete
    sell_out_future.result()
    cedis_future.result()
    tiendas_future.result()

    # Process tables only if all ETL tasks completed successfully
    process_tables()
    
    logger.info("ETL process for OXXO completed successfully.")


if __name__ == "__main__":
    etl_oxxo_flow.serve(name="etl_oxxo")