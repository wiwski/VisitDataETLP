""" Base hooks used to retreive data and context in transform """

from airflow.hooks.base_hook import BaseHook


class VDTransformHook(BaseHook):
    """ Base hook used in the transform process.
    Exposes methods used to retrieve data and context files at the
    beginning of the transform process.
    Can be inherited to use as a transform operator hook by implementing
    :func:`visitdata.models.hooks.vd_transform_hook.VDTransformtHook.retrieve_data`
    and :func:`visitdata.models.hooks.vd_transform_hook.VDTransformtHook.retrieve_context`.
    """

    def get_conn(self):
        """Connect to the Database and get the credentials used to connect to the service.
        """
        raise NotImplementedError()

    def retrieve_files(self):
        """ Wrapper method that fetch both data and context files.
        Returns:
            :return: Both data and context files.
            :rtype: (file, file)
        """
        data = self.retrieve_data()
        context = self.retrieve_context()
        return data, context

    def retrieve_data(self):
        """Fetch data file."""
        raise NotImplementedError()

    def retrieve_context(self):
        """Fetch context file."""
        raise NotImplementedError()
