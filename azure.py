import os
from dotenv import load_dotenv
from fastmcp import FastMCP
from azure.storage.blob import BlobServiceClient

# Load env vars
load_dotenv()

# Get connection string from environment
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
if AZURE_CONNECTION_STRING is None:
    raise ValueError("AZURE_STORAGE_CONNECTION_STRING must be set in environment variables.")

# Init MCP
mcp = FastMCP("Azure MCP Server")

# ---------------- TOOLS ----------------
@mcp.tool()
def list_all_files_in_blob(container_name: str) -> list[str]:
    """
    List all files (blobs) inside a given Azure Blob Storage container.
    Args:
        container_name (str): Name of the blob container
    Returns:
        list[str]: All file (blob) names in the container
    """
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(container_name)

    blobs = container_client.list_blobs()
    return [blob.name for blob in blobs]

# ---------------- RUN ----------------
if __name__ == "__main__":
    mcp.run(transport="streamable-http", host="0.0.0.0", port=8000)
