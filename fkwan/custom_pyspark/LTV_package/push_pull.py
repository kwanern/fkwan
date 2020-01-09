from pyspark.dbutils import DBUtils 
from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark.sparkContext)

adls_base_path = "dbfs:/mnt/workspaces/CMA/Reference/PreProd/Users/fkwan/LTV/"
local_base_path = "file:/Users/fkwan/OneDrive - Starbucks/Data Science/LTV/Code/"
folder = "code"
filename = ["__init__.py", "utils.py", "generate_data.py"]

for i in filename:
    dbutils.fs.rm(adls_base_path+folder+"/"+i)
    dbutils.fs.cp(local_base_path+i, adls_base_path+folder)

dbutils.fs.cp(adls_base_path+"variables", local_base_path+"variables", True)

from azure.datalake.store import core, lib
import adal

directory_id = '03125dd5-d1f8-4c7d-a8b0-52ada8303571'
resource_uri = 'https://centralus.azuredatabricks.net/'
client_id = 'token'
client_secret = 'dapi28fa9388954415aa385b77c45c9dbcc6'

token = lib.auth(
    tenant_id = directory_id,
    client_secret = client_secret,
    client_id = client_id
)