"""VisitData Airflow settings module.
"""
import os
import json
from dotenv import load_dotenv
from airflow import settings
from airflow.models import Connection

from visitdata.models.hooks.vd_rs_hook import VDRSHook

# Load environment variables from .env file
load_dotenv()

session = settings.Session()

# Setting up Visit Data S3 and Redshift Connections
CONNECTIONS = [
    Connection(
        conn_id="VD_S3",
        conn_type="s3",
        login=os.getenv('VD_S3_ACCESS_KEY'),
        password=os.getenv('VD_S3_SECRET_KEY'),
        extra=json.dumps({"default_bucket": os.getenv('VD_S3_DEFAULT_BUCKET')})
    ),

    Connection(
        conn_id="VD_RS",
        conn_type="redshift",
        host=os.getenv('VD_RS_HOST'),
        login=os.getenv('VD_RS_USERNAME'),
        password=os.getenv('VD_RS_PASSWORD'),
        schema=os.getenv('VD_RS_DATABASE'),
        port=os.getenv('VD_RS_PORT')
    )
]

for conn in CONNECTIONS:
    # Removing old connections as Airflow would
    # save multiple connections with same ID
    session.query(Connection).filter(
        Connection.conn_id == conn.conn_id).delete()
    # Add / Re-add the connection
    session.add(conn)
    session.commit()

session.close()
