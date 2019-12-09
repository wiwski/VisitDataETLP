""" Classes used to retrieve and write data with S3 """
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy.orm import sessionmaker
from visitdata.models.hooks.mixins import VDDBMixin


class VDRSHook(PostgresHook, VDDBMixin):
    """ Interact with Visit Data Redshift, to read and write data.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = "VD_RS"

    def create_session(self):
        engine = self.get_sqlalchemy_engine()
        return sessionmaker(bind=engine)()

    def load(self):
        """Load data into DB."""
        raise NotImplementedError()

    def unload(self):
        """Unload data into DB."""
        raise NotImplementedError()

    def retrieve(self):
        """ Retrieve data from DB"""
        raise NotImplementedError()
