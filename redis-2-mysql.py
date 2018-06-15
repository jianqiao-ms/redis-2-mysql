#! /usr/bin/python2
#-* coding: utf-8 -*

# System Package
from __future__ import division
import time

# 3rd Package
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

# Self Package
from config import config
from config import NewDict as dict
from timer import Today
from timer import getstamp
from redisdata import RedisSet
from mysqldata import TimeLine
from mysqldata import MonitorRate
from mysqldata import MonitorAmount


# CONST (Or consider as CONST)
sql_engine = create_engine(
        'mysql+pymysql://{user}:{passwd}@{host}:{port}/{database}?charset=utf8'.format(**config.mysql),
        # echo = True
    )
mysql_conn      =  sql_engine.connect()
mysql_session   = sessionmaker(bind=sql_engine)()
redis_set       = RedisSet(config.redis, config.seek.redis, Today())

default_timeline         = dict()
# default_monitor_rate     = dict()
# default_monitor_amount   = dict()

map(lambda x:default_timeline.__setitem__(x,None), TimeLine.__table__.columns.keys())
# map(lambda x:default_monitor_rate.__setitem__(x,None), MonitorRate.__table__.columns.keys())
# map(lambda x:default_monitor_amount.__setitem__(x,None), MonitorAmount.__table__.columns.keys())

# default_timeline = dict(
#     id                      = None,
#     unique_order_id         = None,
#     qrcode1_generated_at    = None,
#     qrcode1_scanned_at      = None,
#     landingpage_opened_at   = None,
#     sms_requested_at        = None,
#     sms_delivered_at        = None,
#     sms_delivered_status    = None,
#     sms_delivered_message   = None,
#     submit_at               = None,
#     qrcode2_generated_at    = None,
#     create_at               = None,
#     update_at               = None
# )

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

default_monitor_amount = dict(
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
def dump(hash_data):
    try:
        mysql_session.add(TimeLine(**hash_data))
    except IntegrityError as e:
        pass

if __name__ == '__main__':
    start = time.time()
    print('========{}'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))))

    all_hash_data = redis_set.get_hash()
    # 原始数据处理
    map(dump, all_hash_data)
    try:
        mysql_session.commit()
    except Exception as e:
        print("{} - {}".format(e.__class__.__name__, e))
        mysql_session.rollback()


    # Setup MonitorAmount and MonitorRate
    monitor_rate                = default_monitor_rate
    monitor_amount              = default_monitor_amount
    monitor_rate.GEN_DATA_STIME = monitor_amount.GEN_DATA_STIME = all_hash_data[0]['qrcode1_scanned_at']
    monitor_rate.ENABLED        = monitor_amount.ENABLED        = 'Y'
    monitor_rate.CREATE_TIME    = monitor_amount.CREATE_TIME    = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    monitor_rate.CREATE_DATE    = monitor_amount.CREATE_DATE    = Today().__str__()

    monitor_amount.QR1_SCANNER_AMOUNT = len(redis_set.uois)
    monitor_amount.QR2_SCANNER_AMOUNT = 0
    monitor_amount.ALL_PROCESS_TIME   = 0

    for timeline in all_hash_data:
        if timeline.has_key('landingpage_opened_at'):
            monitor_rate.LANDING_RATE +=1 # 打开页面数量
            monitor_rate.QR1SCAN_LANDING_TIME += getstamp(timeline['landingpage_opened_at']) - getstamp(timeline['qrcode1_scanned_at'])

        if timeline.has_key('sms_requested_at'):
            monitor_rate.REQSMS_RATE +=1 # 请求短信数量
            monitor_rate.LANDING_REQSMS_TIME += getstamp(timeline['sms_requested_at']) - getstamp(timeline['landingpage_opened_at'])

        if timeline.has_key('sms_delivered_status') and timeline['sms_delivered_status']=='DELIVERED':
            monitor_rate.SENDSMS_RATE += 1 # 短信成功数量
            monitor_rate.REQSMS_SENDSMS_TIME += getstamp(timeline['sms_delivered_at']) - getstamp(timeline['sms_requested_at'])

        if timeline.has_key('qrcode2_generated_at'):# 全流程完成
            monitor_rate.QR2GEN_RATE += 1 # 全流程完成数量
            monitor_rate.SENDSMS_QR2GEN_TIME += getstamp(timeline['qrcode2_generated_at']) - getstamp(timeline['submit_at'])

            monitor_rate.ALL_PROCESS_TIME += getstamp(timeline['qrcode2_generated_at']) - getstamp(timeline['qrcode1_scanned_at'])
            monitor_rate.GEN_DATA_ETIME = monitor_amount.GEN_DATA_ETIME = timeline['qrcode2_generated_at'] # 拿到最后一个全流程结束时间

    monitor_amount.QR2_SCANNER_AMOUNT   = monitor_rate.QR2GEN_RATE
    monitor_rate.ALL_PROCESS_TIME       = monitor_amount.ALL_PROCESS_TIME = monitor_rate.ALL_PROCESS_TIME / monitor_rate.QR2GEN_RATE #全流程平均耗时时间
    monitor_rate.QR1GEN_SCAN_TIME       = '0' # 第一个码扫描时间与第一个码生成时间在固定时间段内平均值(数据缺失)
    monitor_rate.QR1SCAN_LANDING_TIME   = str(round(monitor_rate.QR1SCAN_LANDING_TIME/monitor_rate.LANDING_RATE, 2)) # 打开着陆页耗时
    monitor_rate.LANDING_REQSMS_TIME    = str(round(monitor_rate.LANDING_REQSMS_TIME/monitor_rate.REQSMS_RATE, 2)) # 请求短信耗时
    monitor_rate.REQSMS_SENDSMS_TIME    = str(round(monitor_rate.REQSMS_SENDSMS_TIME/monitor_rate.SENDSMS_RATE, 2)) # 成功发送短信耗时
    monitor_rate.SENDSMS_QR2GEN_TIME    = str(round(monitor_rate.SENDSMS_QR2GEN_TIME/monitor_rate.QR2GEN_RATE, 2)) # 第二个二维码生成耗时
    monitor_rate.QR2SANNER_TIME         = '0' #扫描第二个二微码时间与生成第二个二微码时间在固定时间内平均值(数据缺失)

    monitor_rate.ALL_PROC_TRAN_RATE     = str(round(monitor_rate.QR2GEN_RATE/len(redis_set.uois), 2)) # 全流程转化率
    monitor_rate.QR2GEN_RATE            = str(round(monitor_rate.QR2GEN_RATE/monitor_rate.SENDSMS_RATE, 2)) # 第二个二维码生成率
    monitor_rate.SENDSMS_RATE           = str(round(monitor_rate.SENDSMS_RATE/monitor_rate.REQSMS_RATE, 2)) # 短信成功率
    monitor_rate.REQSMS_RATE            = str(round(monitor_rate.REQSMS_RATE/monitor_rate.LANDING_RATE, 2)) # 短信请求率
    monitor_rate.LANDING_RATE           = str(round(monitor_rate.LANDING_RATE/len(redis_set.uois), 2)) # 着陆页打开率

    mysql_session.add(MonitorRate(**monitor_rate))
    mysql_session.add(MonitorAmount(**monitor_amount))

    try:
        mysql_session.commit()
    except Exception as e:
        print("{} - {}".format(e.__class__.__name__, e))
        mysql_session.rollback()
    finally:
        mysql_conn.close()
        config.seek.redis = redis_set.lenth
        config.save()

    print('All Done!Toke {} seconds'.format(time.time() - start))

