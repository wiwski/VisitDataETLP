""" Hook mixins used for different ESLP process """


class ExtractMixin:
    """ Expose methods to fetch data during extract process """

    def fetch_data(self):
        """ Describe how to connect to an external
        source and extract the data.
        """
        raise NotImplementedError()


class VDDBMixin:
    """ Expose methods to load, unload and retrieve data in a Database """

    def load(self):
        """ Describe how to load data into a database.
        """
        raise NotImplementedError()

    def unload(self):
        """ Describe how to unload data out of a database.
        """
        raise NotImplementedError()

    def retrieve(self):
        """ Describe how to retrieve data in a database.
        """
        raise NotImplementedError()
