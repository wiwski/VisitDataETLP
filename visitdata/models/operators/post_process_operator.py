""" Post-process base classes in the ELTP process. """

from visitdata.models.operators import ELTPOperator
from visitdata.models.hooks import VDRSHook
from visitdata.models.hooks.mixins import VDDBMixin


class PostProcessOperator(ELTPOperator):
    """ Base class for all post process related steps.
    """
    hook: VDDBMixin = VDRSHook()
    """Basehook: Hook used to retrieve and load data from a database. The hook
    must implement the methods in
    :class:`.visitdata.models.hooks.mixins.VDDBMixin`.
    """

    def __init__(self, *args, hook=None, **kwargs):
        super().__init__(*args, **kwargs)
        if hook:
            self.hook = hook

    def __execute_step(self):
        data = self.__retrieve_transformed_data()
        self.__unload_previous_data(data)
        self.load(data)
        self.post_process(data)
        self.__load_post_processed_data(data)

    def __retrieve_loaded_data(self):
        """ Retrieve data from the load step.
        Returns:
            file: The transformed data.
        """
        data = self.hook.retrieve()
        return data

    def post_process(self, data):
        """ Post process data """
        raise NotImplementedError()

    def __load_post_processed_data(self, data):
        """ Load post-processed data """
        self.hook.load()
