#! /usr/bin/python
#-* coding: utf-8 -*

# System Package
import time

# 3rd Package
from sqlalchemy import Column, String, BigInteger, DateTime, Time, Date
from sqlalchemy.ext.declarative import declarative_base

# Self Package
from config import NewDict

# CONST (Or consider as CONST)
Base = declarative_base()

class DBase():
    def __repr__(self):
        _ = ""
        for key in self.__dict__:
            _+="{}:{},\n ".format(key, self.__dict__[key])
        return "<{}(\n{})>".format(self.__class__.__name__, _)

class TimeLine(Base, DBase):
    __tablename__ = 'timeline_monitoring'
    __table_args__ = {
        "mysql_charset":"utf8"
    }

    id                      = Column(BigInteger, primary_key=True)
    open_id                 = Column(String(32))
    unique_order_id         = Column(String(100),unique=True)
    qrcode1_generated_at    = Column(DateTime)
    qrcode1_scanned_at      = Column(DateTime)
    landingpage_opened_at   = Column(DateTime)
    sms_requested_at        = Column(DateTime)
    sms_delivered_at        = Column(DateTime)
    sms_delivered_status    = Column(String(16))
    sms_delivered_message   = Column(String(255))
    submit_at               = Column(DateTime)
    qrcode2_generated_at    = Column(DateTime)
    create_at               = Column(DateTime)
    update_at               = Column(DateTime)

class MonitorRate(Base, DBase):
    __tablename__ = 'insuracne_monitor_rate'
    __table_args__ = {
        "mysql_charset":"utf8"
    }

    id                      = Column(BigInteger, primary_key=True)
    GEN_DATA_STIME          = Column(DateTime)
    GEN_DATA_ETIME          = Column(DateTime)
    LANDING_RATE            = Column(String(6))
    REQSMS_RATE             = Column(String(6))
    SENDSMS_RATE            = Column(String(6))
    QR2GEN_RATE             = Column(String(6))
    ALL_PROC_TRAN_RATE      = Column(String(6))
    QR1GEN_SCAN_TIME        = Column(BigInteger)
    QR1SCAN_LANDING_TIME    = Column(BigInteger)
    LANDING_REQSMS_TIME     = Column(BigInteger)
    REQSMS_SENDSMS_TIME     = Column(BigInteger)
    SENDSMS_QR2GEN_TIME     = Column(BigInteger)
    QR2SANNER_TIME          = Column(BigInteger)
    ALL_PROCESS_TIME        = Column(BigInteger)
    CREATE_TIME             = Column(DateTime)
    CREATE_DATE             = Column(DateTime)
    ENABLED                 = Column(String(1))

class MonitorAmount(Base, DBase):
    __tablename__ = 'insuracne_monitor_amount'
    __table_args__ = {
        "mysql_charset":"utf8"
    }

    id                      = Column(BigInteger, primary_key=True)
    GEN_DATA_STIME          = Column(DateTime)
    GEN_DATA_ETIME          = Column(DateTime)
    QR1_SCANNER_AMOUNT      = Column(BigInteger)
    QR2_SCANNER_AMOUNT      = Column(BigInteger)
    ALL_PROCESS_TIME        = Column(BigInteger)
    CREATE_TIME             = Column(DateTime)
    CREATE_DATE             = Column(Date)
    ENABLED                 = Column(String(1))

default_monitor_rate = dict(
    id                      = None,
    GEN_DATA_STIME          = None,
    GEN_DATA_ETIME          = None,
    LANDING_RATE            = 0,
    REQSMS_RATE             = 0,
    SENDSMS_RATE            = 0,
    QR2GEN_RATE             = 0,
    ALL_PROC_TRAN_RATE      = 0,
    QR1GEN_SCAN_TIME        = 0,
    QR1SCAN_LANDING_TIME    = 0,
    LANDING_REQSMS_TIME     = 0,
    REQSMS_SENDSMS_TIME     = 0,
    SENDSMS_QR2GEN_TIME     = 0,
    QR2SANNER_TIME          = 0,
    ALL_PROCESS_TIME        = 0,
    CREATE_TIME             = None,
    CREATE_DATE             = None,
    ENABLED                 = None,
)

default_monitor_amount = NewDict(
    id                      = None,
    GEN_DATA_STIME          = None,
    GEN_DATA_ETIME          = None,
    QR1_SCANNER_AMOUNT      = None,
    QR2_SCANNER_AMOUNT      = None,
    ALL_PROCESS_TIME        = None,
    CREATE_TIME             = None,
    CREATE_DATE             = None,
    ENABLED                 = None,
)





# Global Arguments

# Logic

if __name__ == '__main__':
    print(TimeLine.QR2SANNER_TIME           .columns.keys())
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