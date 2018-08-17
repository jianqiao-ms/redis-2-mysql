#! /usr/bin/python2
#-* coding: utf-8 -*

# System Package
import datetime
import logging

# 3rd Package

# Self Package
from config import config

# CONST (Or consider as CONST)
# 程序运行logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

console_logger = logging.StreamHandler()
console_logger.setLevel(logging.DEBUG)
console_fmt = logging.Formatter('[%(asctime)s] %(levelname)-7s [%(funcName)s: %(filename)s]%(lineno)d -- %(message)s')
console_logger.setFormatter(console_fmt)

file_logger = logging.FileHandler('/data/logs/monitor.log')
file_logger.setLevel(logging.INFO)
file_fmt = logging.Formatter('[%(asctime)s] %(levelname)-7s [%(funcName)s: %(filename)s]%(lineno)d -- %(message)s')
file_logger.setFormatter(file_fmt)

logger.addHandler(console_logger)
logger.addHandler(file_logger)

# Class & Functions
if __name__ == '__main__':
    logger.info('开始')

    # 创建链接
    from mysqldata import mysql_handler
    from redisdata import redis_handler

    all_hash_data, new_seek = redis_handler.get_all_hash_data(datetime.date.today(), config.seek.redis)

    if len(all_hash_data) == 0:
        logger.warning('没有需要处理的数据， 程序退出')
        mysql_handler.close()
        exit(1)

    mysql_handler.insert_hash(all_hash_data)
    mysql_handler.calculate(all_hash_data)

    config.seek.redis = new_seek
    config.save()

