#! /usr/bin/python
# -* coding: utf-8 -*

# System Package
import logging
import datetime

# 3rd Package
import redis

# Self Package
from config import config

# CONST (Or consider as CONST)

# Global Arguments

# Logic
logger = logging.getLogger()

class RedisHandler():
    def __init__(self):
        self.conn = redis.Redis(connection_pool=redis.ConnectionPool(**config.redis))
        self.pipe = self.conn.pipeline(transaction=True)
        logger.info('Redis Connected')

    def get_all_hash_data(self, date, seek, end = -1):
        key = "{date}:uois".format(date = date)
        pkey = "{date}:uois".format(date=(date - datetime.timedelta(days=1)))
        sec_diff = (datetime.datetime.now() - datetime.datetime.combine(datetime.date.today(),
                                                                        datetime.time.min)).total_seconds()
        lenth = self.conn.zcard(key)
        logger.info('获取uoi列表')
        uois = self.conn.zrange(key, 0, end) if lenth < seek else self.conn.zrange(key, seek, end)
        if sec_diff<1200:
            uois.extend(self.conn.zrange(pkey, seek, end))
        logger.info('获取uoi列表完成，本次要处理{}条数据'.format(len(uois)))

        if len(uois) == 0:
            return [] ,0

        logger.info('获取hash数据')
        map(self.pipe.hgetall, uois)
        all_hash_data = self.pipe.execute()
        logger.info('获取hash数据完成')

        logger.info('添加uoi到hash数据')
        map(lambda x, y: x.__setitem__('unique_order_id', y), all_hash_data, uois)
        logger.info('添加uoi到hash数据完成')

        return all_hash_data, lenth

redis_handler = RedisHandler()

if __name__ == '__main__':
    pass