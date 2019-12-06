""" Classes used to retrieve and write data with S3 """
from airflow.hooks.S3_hook import S3Hook
from visitdata.models.hooks import VDTransformHook


class VDS3Hook(S3Hook):
    """ Interact with Visit Data AWS S3, to read and write data.
    """

    def get_conn(self):
        """Connect to the Database and get the credentials used
        to connect to the service.
        """
        raise NotImplementedError()

    def fetch_file(self):
        """Fetch data file from S3."""
        # TODO implement
        raise NotImplementedError()

    def write_file(self):
        """Write data file to S3."""
        raise NotImplementedError()
