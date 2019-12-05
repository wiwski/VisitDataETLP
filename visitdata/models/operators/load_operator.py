""" Load base classes in the ELTP process. """

from visitdata.models.operators import ELTPOperator


class LoadOperator(ELTPOperator):
    """Base class for all load related step."""
