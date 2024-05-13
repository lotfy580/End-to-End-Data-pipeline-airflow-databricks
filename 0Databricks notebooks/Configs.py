# Databricks notebook source
def mount_storage(storage_account:str, container:str):

    mount_point = f"/mnt/{storage_account}/{container}"
    source = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"

    if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()): 
        print(f"mount point {mount_point} is already exist!")

    else:
        client_id = dbutils.secrets.get(scope='customertransactions', key='client-id')
        tenant_id = dbutils.secrets.get(scope='customertransactions', key='tenant-id')
        secret_value = dbutils.secrets.get(scope='customertransactions', key='secret-value')

        configs = {
            "fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": secret_value,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
            }
    
    
        dbutils.fs.mount(
            source = source,
            mount_point = mount_point,
            extra_configs = configs
            )
        print("Done!")

# COMMAND ----------

containers = ['raw', 'processed', 'presentaion']
account = 'customertransaction'
for container in containers:
    mount_storage(account, container)

