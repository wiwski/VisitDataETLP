"""
Extract base classes in the ELTP process.
"""
from visitdata.models.operators import ELTPOperator
from visitdata.models.hooks import VDS3Hook
from visitdata.models.hooks.mixins import ExtractMixin


class ExtractOperator(ELTPOperator):
    """ Base class for all extract related steps.
    """

    hook: ExtractMixin = None
    """Basehook: Hook used to extract data from a specific source. The hook
    must implement the methods in
    :class:`.visitdata.models.hooks.mixins.ExtractMixin`.
    """

    def __init__(self, hook, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hook = hook

    def __fetch_data(self):
        """ Call hook to fetch data """
        return self.hook().fetch_file()

    def __write_data(self, data):
        """ Write data to a file """
        # TODO implement
        VDS3Hook().write_file()
        raise NotImplementedError()

    def __write_context(self, data):
        """ Write context to a file """
        # TODO implement
        VDS3Hook().write_file()
        raise NotImplementedError()

    def __execute_step(self):
        super().execute_step()
        self.init_process()
        files = self.__fetch_data()
        self.check_format(files)
        for file in files:
            context = self.create_context(file)
            self.__write_data(file)
            self.__write_context(context)

    def init_process(self):
        """ Initalize extraction process and create dataset in Database """
        # TODO implement initialization logic
        raise NotImplementedError()

    def check_format(self, files):
        """ Check format of extracted files """
        raise NotImplementedError()

    def create_context(self, files):
        """ Create metadata context files """
        raise NotImplementedError()

    def save(self, data, context):
        """ Save data files and context to S3 """
        self.__write_data(data)
        self.__write_context(context)
