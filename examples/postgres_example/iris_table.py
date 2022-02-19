from sqlalchemy import Column, Float, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Iris(Base):
    __tablename__ = 'iris'
    id = Column(Integer, primary_key=True, autoincrement=True)
    sepal_length = Column(Float)
    sepal_width = Column(Float)
    petal_length = Column(Float)
    petal_width = Column(Float)
    species = Column(String)
    dt = Column(DateTime)

def get_base_class():
    return(Base)