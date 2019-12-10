"""
Extract base classes in the ELTP process.
"""
import os
from abc import abstractmethod

from visitdata.models.operators import ELTPOperator
from visitdata.models.hooks import VDS3Hook
from visitdata.models.hooks.mixins import ExtractMixin
from visitdata.models.datasets import VDDataset


class ExtractOperator(ELTPOperator):
    """ Base class for all extract related steps.
    """

    hook: ExtractMixin = None
    """Basehook: Hook used to extract data from a specific source. The hook
    must implement the methods in
    :class:`visitdata.models.hooks.mixins.ExtractMixin`.
    If not provided, :class:`.visitdata.models.hooks.VDS3Hook` will be used.
    """

    def __init__(self, *args, hook=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.hook = hook or VDS3Hook

    def __fetch_data(self):
        """ Call hook to fetch data """
        # TODO: handle multi protocols
        protocol = self.datasource.protocols[0]
        path = (f"{os.getenv('VD_S3_FTP_PREFIX')}"
                f"/client-{self.datasource.organisation_id}"
                f"/{protocol.data_path}")
        return self.hook().fetch_data(
            path=path,
            mask=protocol.data_file
        )

    def __write_data(self, file: VDDataset):
        """ Write data to a file """
        file.save_to_s3(
            key_dest=f"{os.getenv('VD_S3_DATALAKE_PREFIX')}/test/{file.name}")

    def __write_context(self, context: dict):
        """ Write context to a file """
        # TODO implement
        VDS3Hook().write_file()
        raise NotImplementedError()

    def execute_step(self):
        self.init_process()
        files = self.__fetch_data()
        for file in files:
            self.check_format(file)
            context = self.create_context(file)
            self.__write_data(file)
            self.__write_context(context)
        return True

    @abstractmethod
    def init_process(self):
        """ Initalize extraction process and create dataset in Database """
        # TODO implement initialization logic

    @abstractmethod
    def check_format(self, file: VDDataset):
        """ Check format of extracted files """
        raise NotImplementedError()

    @abstractmethod
    def create_context(self, file: VDDataset) -> dict:
        """ Create metadata context files """
        raise NotImplementedError()

    def save(self, data, context):
        """ Save data files and context to S3 """
        self.__write_data(data)
        self.__write_context(context)
