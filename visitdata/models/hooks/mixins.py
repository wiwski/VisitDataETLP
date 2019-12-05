""" Hook mixins used for different ESLP process """


class ExtractMixin:
    """ Expose methods to fetch data during extract process """

    def fetch_data(self):
        """ Describe how to connect to an external
        source and extract the data.
        """
        raise NotImplementedError()
