#! /usr/bin/python
#-* coding: utf-8 -*

# System Package
from __future__ import division
import logging
import datetime
import time

# 3rd Package
from sqlalchemy import Column, String, BigInteger, DateTime, Time, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Self Package
from config import NewDict
from config import config

# CONST (Or consider as CONST)
logger = logging.getLogger()
Base = declarative_base()

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

# Class & Functions
def getstamp(str):
    return int(time.mktime(time.strptime(str, "%Y-%m-%d %H:%M:%S")))

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



class MySQLHandler():
    def __init__(self):
        engine = create_engine(
            'mysql+pymysql://{user}:{passwd}@{host}:{port}/{database}?charset=utf8'.format(**config.mysql),
        )
        self.conn = engine.connect()
        self.session = sessionmaker(bind=engine)()
        logger.info('MySQL Connected')

    def insert_hash(self, all_hash_data):
        logger.info('插入{}条数据'.format(len(all_hash_data)))
        map(self.session.add, map(lambda x:TimeLine(**x), all_hash_data))
        try:
            self.session.commit()
            logger.info('数据插入完成')
        except Exception as e:
            logger.exception(e)
            self.session.rollback()
            
    def calculate(self, all_hash_data):
        logger.info('开始计算数据指标')

        if len(all_hash_data) == 0:
            logger.warning('没有新数据，退出计算任务')
            return

        # Disable old data
        self.conn.execute("UPDATE `insuracne_monitor_amount` SET `ENABLED`='N'")
        self.conn.execute("UPDATE `insuracne_monitor_rate` SET `ENABLED`='N'")

        # Init MonitorAmount and MonitorRate
        monitor_rate = NewDict(**default_monitor_rate)
        monitor_amount = NewDict(**default_monitor_amount)

        # Setup MonitorAmount and MonitorRate
        monitor_rate.GEN_DATA_STIME = monitor_amount.GEN_DATA_STIME = all_hash_data[0]['qrcode1_scanned_at']
        monitor_rate.ENABLED = monitor_amount.ENABLED = 'Y'
        monitor_rate.CREATE_TIME = monitor_amount.CREATE_TIME = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        monitor_rate.CREATE_DATE = monitor_amount.CREATE_DATE = datetime.date.today()

        monitor_amount.QR1_SCANNER_AMOUNT = len(all_hash_data)
        monitor_amount.QR2_SCANNER_AMOUNT = 0
        monitor_amount.ALL_PROCESS_TIME = 0

        for timeline in all_hash_data:
            if timeline.has_key('landingpage_opened_at'):
                monitor_rate.LANDING_RATE += 1  # 打开页面数量
                monitor_rate.QR1SCAN_LANDING_TIME += getstamp(timeline['landingpage_opened_at']) - getstamp(
                    timeline['qrcode1_scanned_at'])

            if timeline.has_key('sms_requested_at'):
                monitor_rate.REQSMS_RATE += 1  # 请求短信数量
                monitor_rate.LANDING_REQSMS_TIME += getstamp(timeline['sms_requested_at']) - getstamp(
                    timeline['landingpage_opened_at'])

            if timeline.has_key('sms_delivered_status') and timeline['sms_delivered_status'] == 'DELIVERED':
                monitor_rate.SENDSMS_RATE += 1  # 短信成功数量
                monitor_rate.REQSMS_SENDSMS_TIME += getstamp(timeline['sms_delivered_at']) - getstamp(
                    timeline['sms_requested_at'])

            if timeline.has_key('qrcode2_generated_at'):  # 全流程完成
                monitor_rate.QR2GEN_RATE += 1  # 全流程完成数量
                monitor_rate.SENDSMS_QR2GEN_TIME += getstamp(timeline['qrcode2_generated_at']) - getstamp(
                    timeline['submit_at'])

                monitor_rate.ALL_PROCESS_TIME += getstamp(timeline['qrcode2_generated_at']) - getstamp(
                    timeline['qrcode1_scanned_at'])
                monitor_rate.GEN_DATA_ETIME = monitor_amount.GEN_DATA_ETIME = timeline[
                    'qrcode2_generated_at']  # 拿到最后一个全流程结束时间

        monitor_amount.QR2_SCANNER_AMOUNT = monitor_rate.QR2GEN_RATE
        monitor_rate.ALL_PROCESS_TIME = monitor_amount.ALL_PROCESS_TIME = monitor_rate.ALL_PROCESS_TIME / monitor_rate.QR2GEN_RATE  # 全流程平均耗时时间
        monitor_rate.QR1GEN_SCAN_TIME = '0'  # 第一个码扫描时间与第一个码生成时间在固定时间段内平均值(数据缺失)
        monitor_rate.QR1SCAN_LANDING_TIME = str(
            round(monitor_rate.QR1SCAN_LANDING_TIME / monitor_rate.LANDING_RATE, 2))  # 打开着陆页耗时
        monitor_rate.LANDING_REQSMS_TIME = str(
            round(monitor_rate.LANDING_REQSMS_TIME / monitor_rate.REQSMS_RATE, 2))  # 请求短信耗时
        monitor_rate.REQSMS_SENDSMS_TIME = str(
            round(monitor_rate.REQSMS_SENDSMS_TIME / monitor_rate.SENDSMS_RATE, 2))  # 成功发送短信耗时
        monitor_rate.SENDSMS_QR2GEN_TIME = str(
            round(monitor_rate.SENDSMS_QR2GEN_TIME / monitor_rate.QR2GEN_RATE, 2))  # 第二个二维码生成耗时
        monitor_rate.QR2SANNER_TIME = '0'  # 扫描第二个二微码时间与生成第二个二微码时间在固定时间内平均值(数据缺失)

        monitor_rate.ALL_PROC_TRAN_RATE = str(
            round(monitor_amount.QR2_SCANNER_AMOUNT / monitor_amount.QR1_SCANNER_AMOUNT, 2))  # 全流程转化率
        monitor_rate.QR2GEN_RATE = str(round(monitor_rate.QR2GEN_RATE / monitor_rate.SENDSMS_RATE, 2))  # 第二个二维码生成率
        monitor_rate.SENDSMS_RATE = str(round(monitor_rate.SENDSMS_RATE / monitor_rate.REQSMS_RATE, 2))  # 短信成功率
        monitor_rate.REQSMS_RATE = str(round(monitor_rate.REQSMS_RATE / monitor_rate.LANDING_RATE, 2))  # 短信请求率
        monitor_rate.LANDING_RATE = str(
            round(monitor_rate.LANDING_RATE / monitor_amount.QR1_SCANNER_AMOUNT, 2))  # 着陆页打开率

        self.session.add(MonitorRate(**monitor_rate))
        self.session.add(MonitorAmount(**monitor_amount))

        try:
            self.session.commit()
            logger.info("数据指标计算完成")
        except Exception as e:
            logger.exception(e)
            self.session.rollback()

    def close(self):
        self.conn.close()

mysql_handler = MySQLHandler()

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