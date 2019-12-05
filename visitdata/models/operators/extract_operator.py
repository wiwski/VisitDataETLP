"""
Extract base classes in the ELTP process.
"""
from visitdata.models.operators.eltp_operator import ELTPOperator
from visitdata.models.hooks.vd_extract_hook import VDExtractHook


class ExtractOperator(ELTPOperator):
    """ Base class for all extract related steps.
    """

    hook: VDExtractHook = None

    def __init__(self, hook, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hook = hook

    def __fetch_data(self):
        """ Call hook to fetch data """
        return self.hook().fetch_data()

    def init_process(self):
        """ Initalize extraction process and create dataset in Database """
        # TODO implement initialization logic
        raise NotImplementedError()

    def execute_step(self):
        super().execute_step()
        self.init_process()
        files = self.__fetch_data()
        self.check_format(files)
        self.create_context(files)

    def check_format(self, files):
        """ Check format of extracted files """
        raise NotImplementedError()

    def create_context(self, files):
        """ Create metadata context files """
        raise NotImplementedError()

    def save(self, files, context_files):
        """ Save data files and context to S3 """
        raise NotImplementedError()
