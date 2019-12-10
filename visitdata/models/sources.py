""" Class declarations for SQLAlchemy to orchestrate tasks and datasources.
"""
import os
from uuid import uuid4
from sqlalchemy import Boolean, Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Organisation(Base):
    __tablename__ = 'organisation'
    id = Column(Integer, primary_key=True)
    datasets = relationship("DatasourceDataset", back_populates="organisation")


class Datasource(Base):
    """ Representation of datasource table. """
    __tablename__ = 'datasource'

    id = Column(Integer, primary_key=True)
    enabled = Column(Boolean)
    interface_type = Column(Integer)
    name = Column(String)
    organisation_id = Column(Integer, ForeignKey("organisation.id"))

    datatasks = relationship("DataTask", back_populates="datasource")
    protocols = relationship("DatasourceProtocol", back_populates="datasource")


class DatasourceProtocol(Base):
    """ Representation of datasource_protocol table. """
    __tablename__ = 'datasource_protocol'

    id = Column(String, primary_key=True)
    name = Column(String)
    enabled = Column(Boolean)
    data_file = Column(String)
    data_path = Column(String)
    data_reload = Column(Integer)
    process_arguments = Column(String)
    protocol_period = Column(String)
    source_access = Column(String)
    source_file = Column(String)
    source_period = Column(String)
    source_sync_last = Column(String)
    source_type = Column(String)

    datasource_id = Column(Integer, ForeignKey("datasource.id"))
    organisation_id = Column(Integer, ForeignKey("organisation.id"))

    datasource = relationship("Datasource", back_populates="protocols")
    datasets = relationship("DatasourceDataset", back_populates="protocol")

    @property
    def source_path(self):
        # TODO implement other sources
        path = (f"{os.getenv('VD_S3_FTP_PREFIX')}"
                f"/client-{self.organisation_id}"
                f"/{self.data_path}")
        return path

    def generate_datalake_path(self, dataset_id: int, step: str, suffix=None):
        """Generate path to save file in Datalake S3.

        Arguments:
            organisation_id {int} -- Organization ID
            suffix {str} -- The rest of the path to append. It is normally
                defined in the protocol.

        Returns:
            str -- A path where the file should be.
        """
        path = (f"{os.getenv('VD_S3_DATALAKE_PREFIX')}"
                f"/{self.datasource_id}"
                f"/{dataset_id}"
                f"/{step}")
        if suffix:
            path += f"/{suffix}"
        return path


class DatasourceDataset(Base):
    """ Representation of datasource_dataset table. """
    __tablename__ = 'datasource_dataset'

    id = Column(String,
                primary_key=True,
                default=lambda x: str(uuid4()),
                auto_increment=False)
    organisation_id = Column(Integer, ForeignKey("organisation.id"))
    datasource_protocol_id = Column(
        Integer, ForeignKey("datasource_protocol.id"))
    data_path_source = Column(String)
    data_path_archive = Column(String)
    process_previous = Column(Integer)
    process_e_timestamp = Column(DateTime)
    process_t_timestamp = Column(DateTime)
    process_l_timestamp = Column(DateTime)
    process_p_timestamp = Column(DateTime)
    process_post_protocol = Column(Integer)
    process_post_source = Column(Integer)

    protocol = relationship(
        "DatasourceProtocol", back_populates="datasets")
    organisation = relationship(
        "Organisation", back_populates="datasets")


class DataTask(Base):
    """ Representation of datahub_task table. """
    __tablename__ = 'datahub_task'

    id = Column(Integer, primary_key=True)
    code = Column(String)
    datasource_id = Column(Integer, ForeignKey("datasource.id"))
    name = Column(String)

    datasource = relationship("Datasource", back_populates="datatasks")
