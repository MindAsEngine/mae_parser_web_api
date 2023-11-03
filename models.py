from sqlalchemy import Column, DateTime, Integer, String, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class Flat(Base):
    __tablename__ = 'flats'

    external_id = Column(Integer, primary_key=True)
    url = Column(String, default='')
    square = Column(Float, default=1.0)
    floor = Column(String, default='')
    total_floor = Column(String, default='')
    address = Column(String, default='')
    repair = Column(String, server_default="repair")
    is_new_building = Column(Boolean, unique=False, default=True)
    room = Column(String, default='')
    created = Column(DateTime(timezone=True), server_default=func.now())
    modified = Column(DateTime(timezone=True), server_default=func.now(), server_onupdate=func.now())
    price_uye = Column(Float, default=1.0)
    price_uzs = Column(Float, default=1.0)
    description = Column(String, default="")
    domain = Column(String, default='')
    is_active = Column(Boolean, unique=False, default=True)
