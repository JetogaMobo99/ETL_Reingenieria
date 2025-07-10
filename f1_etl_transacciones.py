from prefect import flow, task
from prefect.logging import get_run_logger
from etl_scripts import *
from etl_scripts import f1_etl_transacciones_sap
from prefect.context import get_run_context
from utils import *
from notifications import *


@task
def etl_transacciones_sap():
    """ETL process for SAP transactions data"""
    logger = get_run_logger()
    logger.info("Starting ETL process for SAP transactions")
    
    # Call the main function
    f1_etl_transacciones_sap.main()


@flow(log_prints=True,flow_run_name="etl_transacciones_flow")
def etl_transacciones_flow():
    """Main flow for transacciones ETL"""
    etl_transacciones_sap()

@etl_transacciones_flow.on_failure
def handle_failure(flow, flow_run, state):
    notify_failure(flow, flow_run, state=state)

@etl_transacciones_flow.on_completion
def handle_completion(flow, flow_run, state):
    notify_completion(flow, flow_run, state=state)

if __name__ == "__main__":
    etl_transacciones_flow(name="etl_transacciones_flow")