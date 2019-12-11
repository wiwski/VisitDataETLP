"""
Extract base classes in the ELTP process.
"""
import os
from abc import abstractmethod

from visitdata.models.operators import ELTPOperator
from visitdata.models.hooks import VDS3Hook, VDRSHook
from visitdata.models.hooks.mixins import ExtractMixin
from visitdata.models.datasets import VDDataset
from visitdata.models.sources import DatasourceProtocol, DatasourceDataset


class ExtractOperator(ELTPOperator):
    """ Base class for all extract related steps.

    Attributes:
        hook (:class:`airflow.hooks.base_hook.BaseHook`):
            Hook used to extract data from a source.
            The hook must implement the methods in
            :class:`visitdata.models.hooks.mixins.ExtractMixin`.
            If not provided, :class:`visitdata.models.hooks.VDS3Hook`
            will be used.
    """

    hook: ExtractMixin = None

    def __init__(self, *args, hook=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.hook = hook or VDS3Hook

    def __create_dataset(self,
                         protocol: DatasourceProtocol,
                         file: VDDataset) -> DatasourceDataset:
        """Create a datasource_dataset in the DB to store
        information about the process.

        Arguments:
            protocol_id {DatasourceProtocol} -- The ID of the
                datasource_protocol.
            file {VDDataset} -- Extracted file
        Returns:
            :class:`visitdata.models.sources.DatasourceDataset` --
                The saved dataset with an id.
        """
        return self._datasource_hook.save_dataset(
            file.to_datasource_dataset(protocol=protocol)
        )

    def __update_dataset(self, dataset: DatasourceDataset):
        self._datasource_hook.update_dataset(dataset)

    def __fetch_data(self, protocol: DatasourceProtocol):
        """ Call hook to fetch data.

        Arguments:
            file_path {:class:`visitdata.models.sources.DatasourceProtocol`}
                -- The file path as defined in the datasource protocol.
        """
        # TODO: handle multi source
        return self.hook().fetch_data(
            path=protocol.source_path,
            mask=protocol.data_file
        )

    def __write_data(
            self, file: VDDataset, dest_folder: str):
        """ Write data to a file """
        file.save_to_s3(
            key_dest=f"{dest_folder}/{file.name}")

    def __write_context(
            self,
            context: dict,
            dest_folder: str):
        """ Write context to a file """
        VDS3Hook().write_context(
            context=context, key=f"{dest_folder}/context.json")

    def execute_step(self):
        for protocol in self.datasource.protocols:
            files = self.__fetch_data(protocol)
            for file in files:
                self.check_format(file)
                dataset = self.__create_dataset(protocol, file)
                context = self.create_context(file)
                dest_folder = protocol.generate_datalake_path(
                    dataset_id=dataset.id,
                    step="extract")
                self.__write_data(file=file, dest_folder=dest_folder)
                self.__write_context(context=context, dest_folder=dest_folder)
                dataset.data_path_source = dest_folder
                self.__update_dataset(dataset)
        return True

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
