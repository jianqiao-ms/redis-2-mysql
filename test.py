#! /usr/bin/python2
#-* coding: utf-8 -*

# System Package
import time

# 3rd Package
from sqlalchemy import Column, String, BigInteger, DateTime, MetaData, Integer
from sqlalchemy.ext.declarative import declarative_base

# Self Package
from config import config
Base = declarative_base()

# Global Arguments
class DBTest(Base):
    __tablename__ = 'test'

    a = Column(Integer, primary_key=True)
    b = Column(String(100))


# Logic

if __name__ == '__main__':
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(
            'mysql+pymysql://{user}:{passwd}@{host}:{port}/{database}?charset=utf8'.format(**config.mysql),
            # convert_unicode=True,
            # echo=True
        )
    session = sessionmaker(bind = engine)()

    # Base.metadata.create_all(engine)

    a = DBTest(
        b='发送成功'
    )

    session.add(a)
    session.commit()