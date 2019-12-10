""" Classes to provide a common API for different data objects (files, bytes)
across multiple protocols (S3, API, FTP, etc.)."""
import os
import boto3
from datetime import datetime, timezone
from visitdata.models.sources import DatasourceProtocol, DatasourceDataset


class VDDataset():
    """ Base class. Provide abstract methods to implement for each protocol.

    Args:
        name (str): File name or source name of the dataset.
    """

    def __init__(self, name):
        # NOTE: name should be optionnal (for example: API)?
        self.name = name

    def save_to_s3(self, *args, **kwargs):
        """ Handle converting data to a file """
        raise NotImplementedError()

    def to_datasource_dataset(
            self, protocol: DatasourceProtocol) -> DatasourceDataset:
        raise NotImplementedError


class S3VDDataset(VDDataset):
    """ VDDataset for S3 protocol """

    @staticmethod
    def init_from_s3_object(s3_object):
        # TODO: document
        # Add S3VDDataset to s3 object classes because s3 Object is a
        # type created on the fly
        s3_object.__class__ = type(
            "VDS3Object", (S3VDDataset, type(s3_object)), {})
        _, s3_object.name = os.path.split(s3_object.key)
        return s3_object

    def save_to_s3(self, *args, **kwargs):
        """ Copy S3 Object from one bucket to another

        Keyword Arguments:
            key_dest {type} -- Key the object in the new destination.
            bucket_dest {str} -- Name of the bucket to copy the object to.
                (default: Default S3 bucket defined during configuration)
            from_same_account {bool} -- Whether if the source and
                destination buckets are located in the same AWS
                account.
                (default: {True})
        """
        from visitdata.models.hooks import VDS3Hook
        bucket_source = getattr(self, 'bucket_name')
        key_source = getattr(self, 'key')
        bucket_dest = kwargs.get(
            'bucket_dest', os.getenv('VD_S3_DEFAULT_BUCKET'))
        key_dest = kwargs.get('key_dest')
        from_same_account = kwargs.get('key_dest', True)

        if not from_same_account:
            raise NotImplementedError()
        copy_source = {
            'Bucket': bucket_source,
            'Key': key_source
        }
        # pylint: disable=no-member
        bucket = VDS3Hook().get_bucket(bucket_dest)
        bucket.copy(copy_source, key_dest)

    def to_datasource_dataset(
            self, protocol: DatasourceProtocol) -> DatasourceDataset:
        return DatasourceDataset(
            organisation_id=protocol.organisation_id,
            datasource_protocol_id=protocol.id,
            # data_path_source=protocol.generate_datalake_path(
            #    suffix=self.name),
            data_path_archive=getattr(self, 'key'),
            process_e_timestamp=datetime.now(timezone.utc)
        )
