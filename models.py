import datetime

from sqlalchemy import Column, DateTime
from sqlalchemy import Integer
from sqlalchemy import Float,Boolean
from sqlalchemy import String,TypeDecorator,func
from sqlalchemy.ext.declarative import declarative_base

from geoalchemy2 import Geometry

Base = declarative_base()

"""
class TransformedGeometry(TypeDecorator):
    impl = Geometry

    def __init__(self, db_srid, app_srid, **kwargs):
        kwargs["srid"] = db_srid
        self.impl = self.__class__.impl(**kwargs)
        self.app_srid = app_srid

    def column_expression(self, col):
        return self.impl.column_expression(func.ST_Transform(col, self.app_srid))

    def bind_expression(self, bindvalue):
        return func.ST_Transform(self.impl.bind_expression(bindvalue), self.impl.srid)
"""

class Basin_info(Base):
    __tablename__ = 'basin_info'

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    description = Column(String(255))

class Basin(Base):
    __tablename__ = 'basin'
    OBJECTID = Column(Integer)
    disID = Column(Integer)
    Shape_Leng = Column(Float)
    Shape_Area = Column(Float)
    geom = Column(Geometry(geometry_type='MULTIPOLYGON'))
    basin_info_id = Column(Integer)
    basin_shp_id = Column(Integer,primary_key=True)

class Reach(Base):
    __tablename__ = 'reach'
    geom = Column(Geometry(geometry_type='LINESTRING', srid=900913))
    #geom = Column(TransformedGeometry(db_srid=900913, app_srid=4326,geometry_type='LINESTRING'))
    OBJECTID = Column(Integer,primary_key=True)
    ARCID = Column(Integer)
    GRID_CODE = Column(Integer)
    FROM_NODE = Column(Integer)
    TO_NODE = Column(Integer)
    Subbasin = Column(Integer)
    SubbasinR = Column(Integer)
    AreaC = Column(Float)
    Len2 = Column(Float)
    Slo2 = Column(Float)
    Wid2 = Column(Float)
    Dep2 = Column(Float)
    MinEl = Column(Float)
    MaxEl = Column(Float)
    Shape_Leng = Column(Float)
    HydroID = Column(Integer)
    OutletID = Column(Integer)
    basin_id = Column(Integer)
    id = Column(Integer)
    #basin_shp_id = Column(Integer)

class ReachData(Base):
    __tablename__ = 'reach_data'
    Id = Column(Integer,primary_key=True)
    rch = Column(Integer)
    areakm2 = Column(Float)
    flow_outcms = Column(Float)
    wtmpdegc = Column(Float)
    record_month_year_id = Column(Integer)
    is_observed = Column(Boolean)
    basin_id = Column(Integer)
    model_id = Column(Integer)

class RecordDateData(Base):
    __tablename__ = 'record_month_year'
    id = Column(Integer,primary_key=True)
    month = Column(Integer)
    year = Column(Integer)

class User(Base):
    __tablename__ = 'User'

    id = Column(Integer, primary_key=True)
    public_id = Column(String(255))
    username = Column(String(255))
    last_name = Column(String(255))
    first_name = Column(String(255))
    password = Column(String(255))
    institution_name = Column(String(255))

class UserRequests(Base):
    __tablename__ = 'user_request'

    id = Column(Integer, primary_key=True)
    task_id = Column(String(255))
    user_id = Column(Integer)
    arguments = Column(String(255))
    create_time = Column(DateTime, default=datetime.datetime.utcnow())
    status = Column(String(10))
    file_name = Column(String(255))
    error_message = Column(String(255))
    checked_flag = Column(Boolean)



if __name__ == "__main__":
    from sqlalchemy import create_engine
    from dbsetting import DB_URI
    engine = create_engine(DB_URI)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

