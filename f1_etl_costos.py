from prefect import flow, task
from prefect.logging import get_run_logger
from etl_scripts import *
from etl_scripts import f1_etl_costos
from prefect.context import get_run_context
from utils import *
from notifications import *


@flow(log_prints=True,flow_run_name="etl_costos_flow")
def etl_costos_flow():
    """Main flow for costos ETL"""
    f1_etl_costos.main()


@etl_costos_flow.on_failure
def handle_failure(flow, flow_run, state):
    notify_failure(flow, flow_run, state=state)

@etl_costos_flow.on_completion
def handle_completion(flow, flow_run, state):
    notify_completion(flow, flow_run, state=state)

if __name__ == "__main__":
    etl_costos_flow(name="etl_costos_flow")