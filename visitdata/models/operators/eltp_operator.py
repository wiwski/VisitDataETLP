"""
ELTP base class to use as an Airflow operator.
"""
from abc import abstractmethod

from airflow.models import BaseOperator
from visitdata.models.hooks import VDDataflowHook


class ELTPOperator(BaseOperator):
    """ Abstract class for all operators following the
    Extract Load Transform PostProcess pattern.
    """

    datasource = None

    def __init__(self, datahub_task_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.datahub_task_id = datahub_task_id
        # TODO implement init logic

    def execute(self, context):
        """Default execute method called on operator execution. """
        try:
            self.fetch_datasource()
            self.execute_step()
            self.end_process()
        except Exception as error:
            self.on_error(error)

    @abstractmethod
    def execute_step(self):
        """Execution of one of the ELTP step.
        """
        raise NotImplementedError()

    def fetch_datasource(self) -> dict:
        """Fetch datasource information from DataTask.
        The method can be overriden to handle the datasource object manually.

        Returns:
            dict: Datasource object with information about the datasource.
        """
        self.datasource = VDDataflowHook().retrieve_datasource(
            self.datahub_task_id
        )

    def on_error(self, error):
        """Handle error on operator execution. """
        # TODO error handling based on DataTask configuration.
        raise error

    def log_message(self, level, message):
        """Handle log logic. """
        # TODO write logs to DB.
        # TODO send logs to Sentry or whatever.
        # TODO handle via builtin log instead
        raise NotImplementedError()

    def end_process(self):
        """Handle operations at the end of the ELTP step.
        Can be overriden to add data cleaning, result logging...
        """
        raise NotImplementedError()
