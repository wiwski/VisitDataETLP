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

    datatasks = relationship("DataTask", back_populates="datasource")


class DataTask(Base):
    """ Representation of datahub_task table. """
    __tablename__ = 'datahub_task'

    id = Column(Integer, primary_key=True)
    code = Column(String)
    datasource_id = Column(Integer, ForeignKey("datasource.id"))
    name = Column(String)

    datasource = relationship("DataSource", back_populates="datatasks")
