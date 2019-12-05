""" Base hooks for all protocols used in extraction """

from airflow.hooks.base_hook import BaseHook

class VDExtractHook(BaseHook):
    """ Base hook used in the extraction process.
    It handles everything related to connection to remote protocols and fetching data.
    """

    def get_conn(self):
        """Connect to the Database and get the credentials used to connect to the service.
        """
        raise NotImplementedError()

    def fetch_context(self):
        """Connect to the Database and get the credentials used to connect to the service.
        """
        raise NotImplementedError()

    def fetch_data(self):
        """Fetch data and return results, i.e files. """
        raise NotImplementedError()
