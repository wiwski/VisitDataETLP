""" Classes used to retrieve and write data with S3 """
import json
from fnmatch import fnmatch

from airflow.hooks.S3_hook import S3Hook
from visitdata.models.hooks.mixins import ExtractMixin
from visitdata.models.datasets import S3VDDataset


class VDS3Hook(S3Hook, ExtractMixin):
    """ Interact with Visit Data AWS S3, to read and write data.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.aws_conn_id = "VD_S3"
        # Set default_bucket if it has been provided
        # in extra connection configuration
        connection_object = self.get_connection(self.aws_conn_id)
        self.default_bucket = connection_object.extra_dejson.get(
            'default_bucket', None)

    @staticmethod
    def __filter_keys(keys: list, mask: str = None) -> list:
        """Filter s3 keys based on a mask.
        If no mask is provided returns the unfiltered keys.
        Args:
            keys (str): List of s3 keys to filter from.
            mask (str): A regex style string used to filter the keys.
        Returns:
            (list) Filtered list of keys
        """
        if mask:
            return list(filter(
                lambda x: fnmatch(x.split('/')[-1], mask),
                keys
            ))
        return keys

    def fetch_data(self, *args, **kwargs):
        """Fetch files from S3 bucket given a specific path and applying
        a mask to filter files in this path.

        Arguments:
            path {str} -- A path in the bucket where to look for files.

        Keyword Arguments:
            bucket_name {str} -- S3 Bucket name. If no bucket is specified it
                will look in the default bucket specified at runtime.
                (default: {None})
            mask {str} -- A unix file mask used to filter out files.
                (default: {None})

        Returns:
            list -- A list of
                :class:`visitdata.models.datasets.VDDataset`s3 objects
        """
        s3_objects = self.fetch_files(*args, **kwargs)
        return [S3VDDataset.init_from_s3_object(obj) for obj in s3_objects]

    def fetch_files(self, path, bucket_name=None, mask=None):
        """Fetch multiple files from S3
        Args:
            path (str): Path to the S3 object.
            bucket_name (string): Optional bucket name if not the default
            mask (str): Regex pattern used to filter
                out files.
        Returns:
            list(boto3.s3.Object) A list of S3 Objects

        """
        if not bucket_name:
            bucket_name = self.default_bucket
        keys = self.list_keys(
            bucket_name, path)
        if not keys:
            self.log.info(
                f"Nothing found on bucket {bucket_name} with key {path}")
            return []
        filtered_keys = self.__filter_keys(keys, mask)
        files = [self.get_key(key, bucket_name) for key in filtered_keys]
        return files

    def write_context(
            self,
            context: dict,
            key: str,
            bucket_name: str = None
    ):
        """Write context data to s3."""
        if not bucket_name:
            bucket_name = self.default_bucket
        self.load_string(
            string_data=json.dumps(context),
            key=key,
            bucket_name=bucket_name)
