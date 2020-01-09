from datetime import date, datetime, timedelta
from .generate_data import *
from .utils import *
import pytz

# Variables used throughout the script
environment = "prod"
adls_base_path = (
    "/mnt/workspaces/CMA/Reference/PreProd/Teams/data_science/customer-ltv-prod/"
)
TODAY = datetime.now(pytz.timezone("US/Pacific")).date()

#### Main ####
rfm_actual_training = data_pull(
    customer="Non-SR",
    date=TODAY,
    environment=environment,
    adls_base_path=adls_base_path,
)

# Back up RFM training data on ADLS
dbutils.fs.rm(
    adls_base_path + environment + "/previous_runs/" + RUN_DATE + "/data/ltv_training/",
    recurse=True,
)
rfm_actual_training.write.parquet(
    adls_base_path + environment + "/previous_runs/" + RUN_DATE + "/data/ltv_training/"
)

#### Test #####
penalizer = 0.001
