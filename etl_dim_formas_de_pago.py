from prefect import flow, task
from prefect.logging import get_run_logger
from etl_scripts import *
from etl_scripts import dim_formas_de_pago
from prefect.context import get_run_context
from utils import *

# @task
# def etl_dim_formas_de_pago():
#     """ETL process for formas de pago data"""
#     logger = get_run_logger()
#     logger.info("Starting ETL process for formas de pago")
#     etl_formas_de_pago()
#     logger.info("ETL process for formas de pago completed successfully.")

@flow(log_prints=True)
def dim_formas_de_pago_flow():
    """Main flow for formas de pago ETL"""
    dim_formas_de_pago.main()

if __name__ == "__main__":
    dim_formas_de_pago_flow.serve(name="dim_formas_de_pago_flow")