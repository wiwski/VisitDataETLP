""" Class declarations for SQLAlchemy to orchestrate tasks and datasources.
"""

from sqlalchemy import Boolean, Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class DataSource(Base):
    """ Representation of datasource table. """
    __tablename__ = 'datasource'

    id = Column(Integer, primary_key=True)
    enabled = Column(Boolean)
    interface_type = Column(Integer)
    name = Column(String)
    organisation_id = Column(Integer, ForeignKey("organisation.id"))

    datatasks = relationship("DataTask", back_populates="datasource")
    protocols = relationship("DataSourceProtocol", back_populates="datasource")


class DataSourceProtocol(Base):
    """ Representation of datasource_protocol table. """
    __tablename__ = 'datasource_protocol'

    id = Column(Integer, primary_key=True)
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

    datasource = relationship("DataSource", back_populates="protocols")


class DataTask(Base):
    """ Representation of datahub_task table. """
    __tablename__ = 'datahub_task'

    id = Column(Integer, primary_key=True)
    code = Column(String)
    datasource_id = Column(Integer, ForeignKey("datasource.id"))
    name = Column(String)

    datasource = relationship("DataSource", back_populates="datatasks")
