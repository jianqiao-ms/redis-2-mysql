#! /usr/bin/python2
#-* coding: utf-8 -*

# System Package
from __future__ import division
import time
import datetime
import logging
import logging.config

# 3rd Package
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

# Self Package
from config import NewDict
from config import config

from timer import Today

from redisdata import RedisSet

from mysqldata import TimeLine
from mysqldata import MonitorRate
from mysqldata import MonitorAmount

from mysqldata import default_monitor_rate
from mysqldata import default_monitor_amount

# CONST (Or consider as CONST)
# 程序运行logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

console_logger = logging.StreamHandler()
console_logger.setLevel(logging.DEBUG)
console_fmt = logging.Formatter('[%(asctime)s] %(levelname)-7s [%(funcName)s: %(filename)s]%(lineno)d -- %(message)s')
console_logger.setFormatter(console_fmt)

file_logger = logging.FileHandler('monitor.log')
file_logger.setLevel(logging.INFO)
file_fmt = logging.Formatter('[%(asctime)s] %(levelname)-7s [%(funcName)s: %(filename)s]%(lineno)d -- %(message)s')
file_logger.setFormatter(file_fmt)

logger.addHandler(console_logger)
logger.addHandler(file_logger)

sql_engine = create_engine(
        'mysql+pymysql://{user}:{passwd}@{host}:{port}/{database}?charset=utf8'.format(**config.mysql),
        # echo = True
    )


# Class & Functions
def getstamp(str):
    return int(time.mktime(time.strptime(str, "%Y-%m-%d %H:%M:%S")))

def dump(hash_data):
    try:
        mysql_session.add(TimeLine(**hash_data))
    except IntegrityError as e:
        pass

def origin_data(date=datetime.date.today()):
    redis_set = RedisSet(config.redis, config.seek.redis, date)
    # 从redis获取原始数据
    # redis struct
    # {type:zset} [date]:uois => [uoi] || 相关命令zrange zcard
    # {type:hash} [uoi] => 时间点数据 || 相关命令hget hgetall
    all_hash_data = redis_set.get_hash()

    # 写 timeline_monitoring 表
    map(dump, all_hash_data)
    try:
        mysql_session.commit()
    except Exception as e:
        logger.exception(e)
        mysql_session.rollback()

    return all_hash_data

def calculate(all_hash_data):
    # Disable old data
    mysql_conn.execute("UPDATE `insuracne_monitor_amount` SET `ENABLED`='N'")
    mysql_conn.execute("UPDATE `insuracne_monitor_rate` SET `ENABLED`='N'")

    # Init MonitorAmount and MonitorRate
    monitor_rate                = NewDict(**default_monitor_rate)
    monitor_amount              = NewDict(**default_monitor_amount)

    # Setup MonitorAmount and MonitorRate
    monitor_rate.GEN_DATA_STIME = monitor_amount.GEN_DATA_STIME = all_hash_data[0]['qrcode1_scanned_at']
    monitor_rate.ENABLED        = monitor_amount.ENABLED        = 'Y'
    monitor_rate.CREATE_TIME    = monitor_amount.CREATE_TIME    = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    monitor_rate.CREATE_DATE    = monitor_amount.CREATE_DATE    = datetime.date.today()

    monitor_amount.QR1_SCANNER_AMOUNT = len(all_hash_data)
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

    monitor_rate.ALL_PROC_TRAN_RATE     = str(round(monitor_amount.QR2_SCANNER_AMOUNT / monitor_amount.QR1_SCANNER_AMOUNT, 2)) # 全流程转化率
    monitor_rate.QR2GEN_RATE            = str(round(monitor_rate.QR2GEN_RATE/monitor_rate.SENDSMS_RATE, 2)) # 第二个二维码生成率
    monitor_rate.SENDSMS_RATE           = str(round(monitor_rate.SENDSMS_RATE/monitor_rate.REQSMS_RATE, 2)) # 短信成功率
    monitor_rate.REQSMS_RATE            = str(round(monitor_rate.REQSMS_RATE/monitor_rate.LANDING_RATE, 2)) # 短信请求率
    monitor_rate.LANDING_RATE           = str(round(monitor_rate.LANDING_RATE/monitor_amount.QR1_SCANNER_AMOUNT, 2)) # 着陆页打开率

    mysql_session.add(MonitorRate(**monitor_rate))
    mysql_session.add(MonitorAmount(**monitor_amount))

    try:
        mysql_session.commit()
    except Exception as e:
        print("{} - {}".format(e.__class__.__name__, e))
        mysql_session.rollback()
    finally:
        mysql_conn.close()
        config.seek.redis = len(all_hash_data)
        config.save()

if __name__ == '__main__':
    mysql_conn = sql_engine.connect()
    mysql_session = sessionmaker(bind=sql_engine)()

    logger.info('开始')
    logger.info('开始处理原始数据')
    all_hash_data = origin_data()
    logger.info('原始数据处理完成')

    logger.info('开始计算数据指标')
    calculate(all_hash_data)
    logger.info("数据指标计算完成")

