""" Classes used to retrieve and write data with S3 """
from fnmatch import fnmatch

from airflow.hooks.S3_hook import S3Hook


class VDS3Hook(S3Hook):
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

    def fetch_files(self, path, bucket_name=None, mask=None):
        """Fetch multiple files from S3
        Args:
            path (str): Path to the S3 object.
            bucket_name (string): Optional bucket name if not the default
            mask (str): Regex pattern used to filter
                out files.
        Returns:

        """
        if not bucket_name:
            bucket_name = self.default_bucket
        keys = self.list_keys(
            bucket_name, path)
        filtered_keys = self.__filter_keys(keys, mask)
        files = [self.get_key(key, bucket_name) for key in filtered_keys]
        return files

    def fetch_file(self):
        """Fetch data file from S3."""
        # TODO implement
        raise NotImplementedError()

    def write_file(self):
        """Write data file to S3."""
        raise NotImplementedError()
