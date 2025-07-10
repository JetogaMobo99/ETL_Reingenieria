from prefect import flow, task
from prefect.logging import get_run_logger
from etl_scripts import f2_dim_promos
from utils import *




@task
def etl_promos():
    """ETL process for promociones data"""
    logger = get_run_logger()
    logger.info("Starting ETL process for promociones")
    f2_dim_promos.main()
    logger.info("ETL process for promociones completed successfully.")



@flow(log_prints=True)
def dim_promos_flow():
    """Main flow for promociones ETL"""
    etl_promos()


if __name__ == "__main__":
    dim_promos_flow(name="dim_promos_flow")