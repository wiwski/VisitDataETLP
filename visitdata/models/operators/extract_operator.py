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
        self.log.info("Doing extract.")
        self.process_type = "extract"
        self.hook = hook or VDS3Hook

    def __create_dataset(self,
                         protocol: DatasourceProtocol,
                         file: VDDataset) -> DatasourceDataset:
        """Create a datasource_dataset in the DB to store
        information about the process.

        Arguments:
            protocol_id {DatasourceProtocol} -- The ID of the
                datasource_protocol.
            file {:class:`visitdata.models.datasets.VDDataset`}
                -- Extracted file
        Returns:
            :class:`visitdata.models.sources.DatasourceDataset` --
                The saved dataset with an id.
        """
        self.log.info("Protocol %s: creating dataset for file %s",
                      protocol.id,
                      file.name)
        return self._datasource_hook.save_dataset(
            file.to_datasource_dataset(protocol=protocol)
        )

    def __update_dataset(self, dataset: DatasourceDataset):
        self.log.info("Extraction process: Updating dataset %s", dataset.id)
        self._datasource_hook.update_dataset(dataset)

    def __fetch_data(self, protocol: DatasourceProtocol):
        """ Call hook to fetch data.

        Arguments:
            file_path {:class:`visitdata.models.sources.DatasourceProtocol`}
                -- The file path as defined in the datasource protocol.
        """
        self.log.info("Protocol %s: Fetching data at %s with mask %s",
                      protocol.id,
                      protocol.source_path,
                      protocol.data_file)
        # TODO: handle multi source
        return self.hook().fetch_data(
            path=protocol.source_path,
            mask=protocol.data_file
        )

    def __remove_source_data(self, file: VDDataset):
        self.log.info("Cleaning source data...")
        file.remove_source()

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
        protocol: DatasourceProtocol
        for protocol in getattr(self.datasource, 'protocols', []):
            if not protocol.should_execute:
                self.log.info("Skipping extract protocol %s of datasource %s "
                              "with period %s",
                              protocol.id,
                              self.datasource.id,
                              protocol.protocol_period)
                continue
            self.log.info("Starting extraction protocol # %s", protocol.id)
            files = self.__fetch_data(protocol)
            for file in files:
                is_valid = self.check_format(file)
                if not is_valid:
                    # TODO better not valid file exception handling
                    raise Exception(f"File {file.name} invalid.")
                dataset = self.__create_dataset(protocol, file)
                context = self.create_context(file)
                dest_folder = protocol.generate_datalake_path(
                    dataset_id=dataset.id,
                    step="extract")
                self.save_file_and_context(file, context, dest_folder)
                dataset.data_path_source = dest_folder
                self.__update_dataset(dataset)
                self.__remove_source_data(file)
        return True

    def save_file_and_context(self, file, context, dest_folder):
        """ Save data file and context to S3. 

        Arguments:
            file {:class:`visitdata.models.datasets.VDDataset`}
                -- Data file created from extraction.
            context {dict} -- A dict object containing context. Will be
                saved to json format.
            dest_folder {str} -- Destination folder (i.e key) in Datalake S3
        """
        self.log.info("Saving file and context for file {}.", file.name)
        self.__write_data(file=file, dest_folder=dest_folder)
        self.__write_context(context=context, dest_folder=dest_folder)

    def end_process(self):
        super().end_process()
        self.log.info("Extract process ended.")

    @abstractmethod
    def check_format(self, file: VDDataset) -> bool:
        """ Check format of extracted files """
        raise NotImplementedError()

    @abstractmethod
    def create_context(self, file: VDDataset) -> dict:
        """ Create metadata context files """
        raise NotImplementedError()
