""" Transform base classes in the ELTP process. """

from visitdata.models.operators import ELTPOperator
from visitdata.models.hooks import VDS3Hook


class TransformOperator(ELTPOperator):
    """ Base class for all transform related steps.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __retrieve_extracted_data(self):
        """ Retrieve files from the extract step.
        Returns:
            files: The data and the context files.
        """
        # TODO implementation with S3 Hook
        VDS3Hook().fetch_file()
        return None, None

    def __save_transformed_data(self, data):
        """ Save transformed data to a CSV file.
        """
        # TODO save transformed data.
        raise NotImplementedError()

    def __execute_step(self):
        super().execute_step()
        data, context = self.__retrieve_extracted_data()
        transformed_data = self.transform(data, context)
        self.check_format(transformed_data)
        self.__save_transformed_data(data)

    def transform(self, data, context=None):
        """ Transformation logic of data with optional context.
        """
        raise NotImplementedError()

    def check_format(self, data):
        """ Check format of transformed data.
        Returns:
            bool: whether or not the format is right.
        """
        raise NotImplementedError()

    def create_context(self, files):
        """ Create metadata context files. """
        raise NotImplementedError()
