#! /usr/bin/python
#-* coding: utf-8 -*

# System Package
import time

# 3rd Package
from sqlalchemy import Column, String, BigInteger, DateTime, Table
from sqlalchemy.ext.declarative import declarative_base

# Self Package
from config import config

# CONST (Or consider as CONST)
Base = declarative_base()

class TimeLine(Base):
    __tablename__ = 'timeline_monitoring'
    __table_args__ = {
        "mysql_charset":"utf8"
    }

    id                      = Column(BigInteger, primary_key=True)
    unique_order_id         = Column(String(100),unique=True)
    qrcode1_generated_at    = Column(DateTime)
    qrcode1_scanned_at      = Column(DateTime)
    landingpage_opened_at   = Column(DateTime)
    sms_requested_at        = Column(DateTime)
    sms_delivered_at        = Column(DateTime)
    sms_delivered_status    = Column(String(16))
    sms_delivered_message   = Column(String(255), default="发送成功")
    submit_at               = Column(DateTime)
    qrcode2_generated_at    = Column(DateTime)
    create_at               = Column(DateTime)
    update_at               = Column(DateTime)

    def __repr__(self):
        _ = ""
        for key in self.__dict__:
            _+="{}:{},\n ".format(key, self.__dict__[key])
        return "<{}(\n{})>".format(self.__class__.__name__, _)

# Global Arguments

# Logic

if __name__ == '__main__':
    print(TimeLine.__table__.columns.keys())
    # from sqlalchemy import create_engine
    # from sqlalchemy.orm import sessionmaker
    #
    # mysql_session = sessionmaker(bind=create_engine(
    #         'mysql+mysqlconnector://{user}:{passwd}@{host}:{port}/{database}?charset=utf8'.format(**config.mysql),
    #         # convert_unicode=True,
    #         # echo=True
    #     )
    # )()

    # a = TimeLine(**dict(
    # id                      = None,
    # unique_order_id         = 'test',
    # qrcode1_generated_at    = None,
    # qrcode1_scanned_at      = None,
    # landingpage_opened_at   = None,
    # sms_requested_at        = None,
    # sms_delivered_at        = None,
    # sms_delivered_status    = None,
    # sms_delivered_message   = u"发送成功",
    # submit_at               = None,
    # qrcode2_generated_at    = None,
    # create_at               = None,
    # update_at               = None
    # ))
    # print(a)
    #
    # print(str(a.insert()))
    #
    # mysql_session.add(a)
    # print()
    # mysql_session.commit()

    # items = mysql_session.query(TimeLine).all()
    # a = items[0].sms_delivered_message
    # print(a)
    # print(a.encode('utf8').decode())
    # print(type(a))