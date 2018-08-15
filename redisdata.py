#! /usr/bin/python
#-* coding: utf-8 -*

# System Package
import time
import logging
import datetime

# 3rd Package
import redis

# Self Package

# CONST (Or consider as CONST)

# Global Arguments

# Logic
logger = logging.getLogger()

class RedisSet():
    def __init__(self, _config, _seek, _today = datetime.date.today()):
        pool = redis.ConnectionPool(**_config)
        self.conn = redis.Redis(connection_pool=pool)
        # self.conn = redis.Redis(**_config)
        self.redis_pipe = self.conn.pipeline(transaction=True)
        self.seek = _seek
        self.key = "{date}:uois".format(date = _today)
        self.pkey = "{date}:uois".format(date = (_today - datetime.timedelta(days=1)))

        self.lenth = self.len_set()
        self.uois = self.range_uois() if self.lenth<self.seek else self.range_uois(self.seek, -1)
        self.seek = self.lenth

    def len_set(self):
        return self.conn.zcard(self.key)

    def range_uois(self, start=0, end=-1):
        uois = self.conn.zrange(self.key, start, end)
        if start==0 and end == -1 and self.seek != 0:
            yesterday_uois = self.conn.zrange(self.pkey, self.seek, -1)
            uois.extend(yesterday_uois)
        return uois

    def get_hash(self):
        # type(result) list
        # [dict({hgetall(uoi)})]

        logger.info('从redis获取原始数据')
        map(self.redis_pipe.hgetall, self.uois)
        result = self.redis_pipe.execute()
        map(lambda x,y:x.__setitem__('unique_order_id', y), result, self.uois)
        logger.info('Done!Read {:10} lines data.Now zset len {}'.format(len(self.uois), self.lenth))

        return result

if __name__ == '__main__':
    from config import config
    from timer import Today
    redis_set = RedisSet(config.redis, 0)

    for uoi in redis_set.uois:
        hash_data = redis_set.conn.hgetall(uoi)
        print(hash_data)
        for key in hash_data:
            print("{} : {}".format(key, hash_data[key]))

    config.seek.redis = len(redis_set.lenth)

