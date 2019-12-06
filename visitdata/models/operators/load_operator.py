""" Load base classes in the ELTP process. """

from visitdata.models.operators import ELTPOperator
from visitdata.models.hooks import VDS3Hook
from visitdata.models.hooks.mixins import VDDBMixin


class LoadOperator(ELTPOperator):
    """Base class for all load related steps."""

    hook: VDDBMixin = None
    """Basehook: Hook used to load and unload data from a database. The hook
    must implement the methods in
    :class:`.visitdata.models.hooks.mixins.VDDBMixin`.
    """

    def __init__(self, hook, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hook = hook

    def __execute_step(self):
        data = self.__retrieve_transformed_data()
        self.__unload_previous_data(data)
        self.load(data)

    def __retrieve_transformed_data(self):
        """ Retrieve files from the transform step.
        Returns:
            file: The transformed data.
        """
        # TODO implementation with S3 Hook
        VDS3Hook().fetch_file()
        return None, None

    def __unload_previous_data(self, data):
        """ Clean data in the database
        if it was previous loaded.
        """
        raise NotImplementedError()

    def load(self, data):
        """ Insert data into the Database."""
        raise NotImplementedError()

    def verify(self):
        """ Verify data has been loaded correctly.
        """
