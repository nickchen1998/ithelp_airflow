from sqlalchemy import Column, VARCHAR, DateTime, INTEGER, TEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func


Base = declarative_base()


class Exhibitions(Base):
    __tablename__ = "exhibitions"
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    name = Column(VARCHAR, nullable=False)
    start_time = Column(DateTime(timezone=True), nullable=False, default=func.now())
    end_time = Column(DateTime, nullable=False)
    location = Column(VARCHAR, nullable=False)
    detail = Column(TEXT, nullable=True)
    url = Column(VARCHAR, nullable=False)
