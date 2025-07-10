from prefect import flow, task
from prefect.logging import get_run_logger
from etl_scripts import *
from etl_scripts import f2_dim_formas_de_pago
from prefect.context import get_run_context
from utils import *
from notifications import *



@flow(log_prints=True,flow_run_name="dim_formas_de_pago_flow")
def dim_formas_de_pago_flow():
    """Main flow for formas de pago ETL"""
    f2_dim_formas_de_pago.main()


@dim_formas_de_pago_flow.on_failure
def handle_failure(flow, flow_run, state):
    notify_failure(flow, flow_run, state=state)

@dim_formas_de_pago_flow.on_completion
def handle_completion(flow, flow_run, state):
    notify_completion(flow, flow_run, state=state)

if __name__ == "__main__":
    dim_formas_de_pago_flow(name="dim_formas_de_pago_flow")