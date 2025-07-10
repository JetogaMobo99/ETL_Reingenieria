from prefect import flow, task
from prefect.logging import get_run_logger
from etl_scripts import *
from etl_scripts import f1_etl_costos
from prefect.context import get_run_context
from etl_scripts import f1_etl_transacciones_sap
from utils import *
from notifications import *



@task
def etl_costos():
    """ETL process for costos data"""
    logger = get_run_logger()
    logger.info("Starting ETL process for costos")
    
    # Call the main function
    f1_etl_costos.main()

@task
def etl_transacciones():
    """ETL process for transacciones data"""
    logger = get_run_logger()
    logger.info("Starting ETL process for transacciones")
    
    # Call the main function
    f1_etl_transacciones_sap.main()


@flow(log_prints=True, flow_run_name="etl_all_flows")
def etl_all_flows():
    """Main flow for all ETL processes"""
    costos=etl_costos.submit()
    transacciones = etl_transacciones.submit()

    costos.result()
    transacciones.result()

@etl_all_flows.on_failure
def handle_failure(flow, flow_run, state):
    notify_failure(flow, flow_run, state=state)

@etl_all_flows.on_completion
def handle_completion(flow, flow_run, state):
    notify_completion(flow, flow_run, state=state)