""" Classes used to retrieve data in the transform process with S3 """
from airflow.hooks.S3_hook import S3Hook
from visitdata.models.hooks import VDTransformHook


class VDTransformS3Hook(VDTransformHook, S3Hook):
    """Interact with AWS S3, to retrieve data in the transform process. """

    def retrieve_data(self):
        # TODO implement
        raise NotImplementedError()

    def retrieve_context(self):
        # TODO implement
        raise NotImplementedError()
