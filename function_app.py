import logging, os
import azure.functions as func
from azure.storage.blob import ContainerClient, BlobClient


app = func.FunctionApp()

def create_storage_container_client(blob_folder_path):
    logging.info(f"Creating container client from blob (with full virtual path) '{blob_folder_path}'")
    connection_string = os.environ.get("AzureWebJobsStorage")
    container_name = blob_folder_path.split('/')[0]

    return ContainerClient.from_connection_string(connection_string, container_name=container_name)

def read_and_process_blobs(container_client, blob_virtual_path):
    blob_path_without_container = '/'.join(blob_virtual_path.split('/')[1:-1])
    logging.info(F"Reading files in within specified container under the path '{blob_path_without_container}'")
    blobs = container_client.list_blobs()
    for blob in [blob for blob in blobs if blob.name.startswith(blob_path_without_container)]:
        blob_name = blob.name.split('/')[-1]
        as_bytes = container_client.download_blob(blob).content_as_bytes()
        logging.info(F"Reading blob '{blob_name}'")

@app.function_name(name="BlobTrigger")
@app.blob_trigger(arg_name="theblob", path="a-foo-bar-container/drop-here/{name}",
                  connection="AzureWebJobsStorage")

def run_this(theblob: func.InputStream):
    container_client = create_storage_container_client(theblob.name)
    read_and_process_blobs(container_client, theblob.name)