from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class CommunityDimension(Base):
    __tablename__ = 'community_dim'
    id = Column(Integer, primary_key=True)
    name = Column(String)
