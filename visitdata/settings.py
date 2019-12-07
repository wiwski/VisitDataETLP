"""VisitData Airflow settings module.
"""
import os
from dotenv import load_dotenv
from airflow import settings
from airflow.models import Connection

# Load environment variables from .env file
load_dotenv()

# Setting up Visit Data S3 Connection
VD_S3_CON_ID = "VD_S3"
VD_S3_CONNECTION = Connection(
    conn_id="VD_S3",
    conn_type="s3",
    login=os.getenv('VD_S3_ACCESS_KEY'),
    password=os.getenv('VD_S3_SECRET_KEY'))
session = settings.Session()
# Removing old connections as Airflow save
session.query(Connection).filter(Connection.conn_id == VD_S3_CON_ID).delete()
session.add(VD_S3_CONNECTION)
session.commit()
session.close()
