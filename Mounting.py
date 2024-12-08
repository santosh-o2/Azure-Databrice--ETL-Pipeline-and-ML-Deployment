
storage_account_name = "santoshdb8"  
container_name = "data-container"         
sas_token = "****************"              


dbutils.fs.mount(
    source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point="/mnt/blob_storage",
    extra_configs={
        f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net": sas_token
    }
)

print("Mounted storage successfully!")
