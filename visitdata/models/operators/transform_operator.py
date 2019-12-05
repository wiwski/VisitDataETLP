""" Transform base classes in the ELTP process. """

from visitdata.models.operators import ELTPOperator
from visitdata.models.hooks import VDTransformHook, VDTransformS3Hook


class TransformOperator(ELTPOperator):
    """ Base class for all transform related steps.
    """

    hook: VDTransformHook = VDTransformS3Hook()

    def __init__(self, *args, hook=None, **kwargs):
        super().__init__(*args, **kwargs)
        if hook:
            self.hook = hook()

    def __retrieve_extracted_data(self):
        """ Retrieve files from the extract step.
        Returns:
            files: The data and the context files.
        """
        # TODO implementation with S3 Hook
        return None, None

    def __save_transformed_data(self, data):
        """ Save transformed data to a file.
        """
        # TODO save transformed data.
        raise NotImplementedError()

    def execute_step(self):
        super().execute_step()
        data, context = self.__retrieve_extracted_data()
        self.check_format(data)
        self.__save_transformed_data(data)

    def check_format(self, data):
        """ Check format of transformed data.
        Returns:
            bool: whether or not the format is right.
        """
        raise NotImplementedError()

    def create_context(self, files):
        """ Create metadata context files. """
        raise NotImplementedError()

    def save(self, files, context_files):
        """ Save data files and context to S3. """
        raise NotImplementedError()
