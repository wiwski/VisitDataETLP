""" Classes used to retrieve and write data with S3 """
from airflow.contrib.hooks.redshift_hook import RedshiftHook
from visitdata.models.hooks.mixins import VDDBMixin


class VDRSHook(RedshiftHook, VDDBMixin):
    """ Interact with Visit Data AWS S3, to read and write data.
    """

    def get_conn(self):
        """Connect to the Database and get the credentials used
        to connect to the service.
        """
        raise NotImplementedError()

    def load(self):
        """Load data into DB."""
        raise NotImplementedError()

    def unload(self):
        """Unload data into DB."""
        raise NotImplementedError()

    def retrieve(self):
        """ Retrieve data from DB"""
        raise NotImplementedError()
