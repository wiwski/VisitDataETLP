""" Classes used to retrieve tasks and data sources information necessary
to run Airflow and the ELTP processes.
"""
from visitdata.models.sources import DataTask, DatasourceDataset

from visitdata.models.hooks import VDRSHook


class VDDataflowHook(VDRSHook):
    """ Hook used to make a connection to the database holding the information
    about Airflow tasks, VisitData data sources, and ETLP processes.
    """

    def retrieve_datasource(self, datatask_id):
        """ Retrieve a datasource object."""
        session = self.create_session()
        task = session.query(DataTask).filter(
            DataTask.id == datatask_id).first()
        # session.close()
        return task.datasource

    def save_dataset(self, dataset: DatasourceDataset) -> DatasourceDataset:
        """ Insert a datasource_dataset to the DB.

        Arguments:
            dataset {:class:`visitdata.models.sources.DatasourceDataset`}
                -- The :class:`visitdata.models.sources.DatasourceDataset`
                to save.

        Returns:
            :class:`visitdata.models.sources.DatasourceDataset` -- The
                created datasource_dataset.
        """
        session = self.create_session()
        session.add(dataset)
        session.commit()
        session.close()
        return dataset
