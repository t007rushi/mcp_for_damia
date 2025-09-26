import os
import base64
import snowflake.connector
import psycopg2
from cryptography.hazmat.primitives import serialization
from fastmcp import FastMCP
from dotenv import load_dotenv

# ---------------- LOAD ENV ----------------
load_dotenv()


# Create MCP Server
mcp = FastMCP("Jira MCP Server")

# Snowflake config
# ---------------- ENV VARS ----------------
SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA") # .p8 or PEM file
SNOWFLAKE_PRIVATE_KEY_B64 = os.getenv("SNOWFLAKE_PRIVATE_KEY_B64")  # base64-encoded private key

POSTGRES_HOST     = os.getenv("POSTGRES_HOST")
POSTGRES_DB       = os.getenv("POSTGRES_DB")
POSTGRES_USER     = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))

# ---------------- DECODE PRIVATE KEY ----------------
if SNOWFLAKE_PRIVATE_KEY_B64 is None:
    raise RuntimeError("SNOWFLAKE_PRIVATE_KEY_B64 environment variable must be set.")
private_key_pem = base64.b64decode(SNOWFLAKE_PRIVATE_KEY_B64)
p_key = serialization.load_pem_private_key(
    private_key_pem,
    password=None
)
private_key = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)


# ---------------- TOOL ----------------
@mcp.tool()
def validation_count_snowflake_postgres() -> str:
    """
    Validates bronze table record counts between Snowflake and Postgres.
    Returns a comparison summary.
    """
    # ---- Connect Snowflake ----
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        private_key=private_key,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    cursor = conn.cursor()

    cursor.execute("SELECT count(*) FROM orders_raw")
    order_raw_count = cursor.fetchone()[0] # type: ignore

    cursor.execute("SELECT count(*) FROM customers_raw")
    customers_raw_count = cursor.fetchone()[0] # type: ignore

    # ---- Connect Postgres ----
    pg_connection = psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT,
    )
    cur = pg_connection.cursor()

    cur.execute("SELECT count(*) FROM bronze.orders_raw")
    postg_order_raw_count = cur.fetchone()[0] # type: ignore

    cur.execute("SELECT count(*) FROM bronze.customers_raw")
    postg_customers_raw_count = cur.fetchone()[0] # type: ignore

    return (
        f"Snowflake Orders: {order_raw_count}, Customers: {customers_raw_count} | "
        f"Postgres Orders: {postg_order_raw_count}, Customers: {postg_customers_raw_count}"
    )
# --------------- RUN MCP ----------------
if __name__ == "__main__":
    mcp.run(transport="streamable-http", host="0.0.0.0", port=8000)
